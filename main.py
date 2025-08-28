import io, zipfile, re, os
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse
from datetime import datetime
import fitz  # PyMuPDF
from supabase import create_client, Client

app = FastAPI()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE")
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

MONEY = re.compile(r"^\$?\d{1,3}(?:,\d{3})*(?:\.\d{2})-?$")
INT   = re.compile(r"^\d{1,4}$")
CODE  = re.compile(r"^[A-Za-z0-9][A-Za-z0-9\-\/\.]*$")

def norm_date(s:str)->str:
    for fmt in ("%Y-%m-%d","%m/%d/%Y","%m/%d/%y"):
        try: return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except: pass
    return ""

def invnum(t:str, fname:str):
    m = re.search(r"\b(\d{6,9}-\d{2})\b", t)
    if m: return m.group(1)
    m = re.search(r"\bINV(?:OICE)?\s*#?\s*([A-Z0-9\-]{6,})\b", t, re.I)
    if m: return m.group(1)
    return os.path.splitext(os.path.basename(fname))[0]

def invdate(t:str, fname:str):
    m = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", t)
    if m: return m.group(1)
    m = re.search(r"\b(\d{2}/\d{2}/\d{2,4})\b", t)
    if m: return norm_date(m.group(1))
    m = re.search(r"_(20\d{2})(\d{2})(\d{2})_", fname)
    if m: y,mm,dd=m.groups(); return f"{y}-{mm}-{dd}"
    return ""

def supplier_name(t:str):
    return "Noble" if re.search(r"\bNoble\b", t, re.I) else ""

def words_to_lines(words, y_tol=1.4):
    words = sorted(words, key=lambda w: (round(w[1],1), w[0]))
    lines=[]; cur=[]; cury=None
    for w in words:
        y=w[1]
        if cury is None or abs(y-cury)<=y_tol:
            cur.append(w); cury=y if cury is None else cury
        else:
            lines.append(sorted(cur, key=lambda t:t[0])); cur=[w]; cury=y
    if cur: lines.append(sorted(cur, key=lambda t:t[0]))
    return lines

def money_val(s):
    s=s.replace("$","").replace(",","").strip()
    neg = s.endswith("-")
    if neg: s=s[:-1]
    try: v=float(s); return -v if neg else v
    except: return None

def parse_items(page):
    out=[]
    lines = words_to_lines(page.get_text("words"))
    for i, line in enumerate(lines):
        toks=[w[4] for w in line]
        if not toks: continue
        mpos = [k for k,t in enumerate(toks) if MONEY.match(t)]
        if not mpos: continue
        total_i = mpos[-1]; total_txt = toks[total_i]
        unit_i = next((k for k in range(total_i-1,-1,-1) if MONEY.match(toks[k])), None)
        if unit_i is None: continue
        qty_i = next((k for k in range(unit_i-1,-1,-1) if INT.match(toks[k])), None)
        if qty_i is None: continue
        code_i = 0
        if INT.match(toks[0]) and len(toks)>1: code_i=1
        code = toks[code_i].strip()
        if not CODE.match(code): continue

        # Name under code if next line has no money tokens
        item_name = ""
        if i+1 < len(lines):
            nxt = [w[4] for w in lines[i+1]]
            if not any(MONEY.match(t) for t in nxt):
                item_name = " ".join(nxt).strip(" -/")

        if not item_name:
            qty_x0 = line[qty_i][0]; code_x1 = line[code_i][2]
            seg = [w[4] for w in line if (w[0] >= code_x1-1 and w[2] <= qty_x0+1)]
            item_name = " ".join(seg[1:]).strip(" -/") if len(seg)>1 else ""

        try:
            qty  = int(toks[qty_i].replace(",",""))
            unit = money_val(toks[unit_i]); tot = money_val(total_txt)
            if unit is None or tot is None or qty<=0: continue
            if abs(qty*unit - tot) > 0.25*max(abs(tot),1.0): continue
            out.append((code.lower(), item_name, qty, unit, tot))
        except: pass
    return out

@app.get("/health")
def health(): return {"ok": True}

@app.post("/ingest")
async def ingest(files: list[UploadFile] = File(...)):
    parsed_lines=[]; invoices_rows=[]; errors=[]
    seen_invoices=set(); files_count=0

    for f in files:
        try:
            blob = await f.read()
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                for name in z.namelist():
                    if not name.lower().endswith(".pdf"): continue
                    files_count += 1
                    pdf = io.BytesIO(z.read(name))
                    try: doc = fitz.open(stream=pdf, filetype="pdf")
                    except Exception as e:
                        errors.append(f"{name}: pdf open failed {e}"); continue

                    text_all = "\n".join(p.get_text("text") for p in doc)
                    inv_no = invnum(text_all, name)
                    inv_date = invdate(text_all, name)
                    supp = supplier_name(text_all)
                    if inv_no in seen_invoices:
                        doc.close(); continue
                    seen_invoices.add(inv_no)

                    total_cost=0; total_qty=0; item_ct=0
                    for p in doc:
                        for code, nm, qty, unit, tot in parse_items(p):
                            item_ct += 1
                            total_cost += tot
                            total_qty  += qty
                            parsed_lines.append({
                                "supplier": supp,
                                "invoice_number": inv_no,
                                "invoice_date": inv_date,
                                "item_code": code,
                                "item_name": nm,
                                "quantity": qty,
                                "unit_price": round(unit,2),
                                "line_total": round(tot,2),
                                "file_name": os.path.basename(name)
                            })
                    doc.close()
                    if item_ct>0:
                        invoices_rows.append({
                            "supplier": supp,
                            "invoice_number": inv_no,
                            "invoice_date": inv_date or None,
                            "lines": total_qty,
                            "total_value": round(total_cost,2),
                            "file_name": os.path.basename(name)
                        })
        except Exception as e:
            errors.append(f"{f.filename}: zip read failed {e}")

    # Upsert to Supabase
    if invoices_rows:
        sb.table("invoices").upsert(invoices_rows, on_conflict="invoice_number").execute()
    if parsed_lines:
        sb.table("items").insert(parsed_lines).execute()

    # Build master/monthly for convenience (client can also query Supabase view)
    # Master
    from collections import defaultdict
    by = defaultdict(lambda: {"qty":0,"val":0,"invs":set(),"name_counts":{}, "first":"","last":""})
    for r in parsed_lines:
        k = r["item_code"]
        by[k]["qty"] += r["quantity"]
        by[k]["val"] += r["line_total"]
        if r["invoice_number"]: by[k]["invs"].add(r["invoice_number"])
        nm=r["item_name"].strip() if r["item_name"] else ""
        if nm: by[k]["name_counts"][nm]=by[k]["name_counts"].get(nm,0)+1
        d=r["invoice_date"] or ""
        if d:
            by[k]["first"] = min(filter(None,[by[k]["first"],d])) if by[k]["first"] else d
            by[k]["last"]  = max(filter(None,[by[k]["last"],d]))  if by[k]["last"]  else d

    def months_between(a,b):
        if not a or not b: return 1
        ay,am = map(int,a.split("-")[:2]); by,bm = map(int,b.split("-")[:2])
        return max(1,(by-ay)*12+(bm-am)+1)

    master=[]
    for k,v in by.items():
        best_name = sorted(v["name_counts"].items(), key=lambda t:t[1], reverse=True)[0][0] if v["name_counts"] else ""
        invs=len(v["invs"]); span=months_between(v["first"], v["last"])
        master.append({
            "item_code": k,
            "item_name": best_name,
            "invoices": invs,
            "frequency_per_month": round(invs/span,2),
            "total_quantity": v["qty"],
            "total_value": round(v["val"],2),
            "avg_price": round(v["val"]/v["qty"],2) if v["qty"] else 0.0,
            "last_invoice_date": v["last"] or ""
        })
    master.sort(key=lambda r:r["total_value"], reverse=True)

    # Monthly summary
    from collections import Counter
    monthly_counter_q = Counter()
    monthly_counter_v = Counter()
    for r in parsed_lines:
        m = (r["invoice_date"] or "")[:7]
        if len(m)==7:
            monthly_counter_q[m] += r["quantity"]
            monthly_counter_v[m] += r["line_total"]
    months = sorted(monthly_counter_q.keys())
    monthly=[]
    prev_q=prev_v=None
    for m in months:
        q=monthly_counter_q[m]; v=round(monthly_counter_v[m],2)
        row={"invoice_month":m,"total_quantity":q,"total_value":v,"qty_mom_pct":None,"val_mom_pct":None}
        if prev_q not in (None,0): row["qty_mom_pct"]=round((q-prev_q)/prev_q,3)
        if prev_v not in (None,0): row["val_mom_pct"]=round((v-prev_v)/prev_v,3)
        monthly.append(row); prev_q,prev_v=q,v

    return JSONResponse({
        "parsed_lines": parsed_lines,
        "master": master,
        "monthly": monthly,
        "oddities": [],  # add later if you want
        "files_processed": files_count,
        "errors": errors
    })
