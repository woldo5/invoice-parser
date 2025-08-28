import io, zipfile, re, os, json, httpx
from datetime import datetime
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse
import fitz  # PyMuPDF
from supabase import create_client, Client

app = FastAPI()

import os, httpx

@app.get("/check-openai")
def check_openai():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return {"ok": False, "error": "OPENAI_API_KEY not set in environment"}
    try:
        r = httpx.post(
            "https://api.openai.com/v1/models",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type":"application/json"},
            timeout=20
        )
        r.raise_for_status()
        data = r.json()
        first = data["data"][0]["id"] if "data" in data and data["data"] else None
        return {"ok": True, "first_model": first}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ---------- CONFIG ----------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
sb = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL and SUPABASE_KEY else None

# ---------- Lightweight helpers ----------
MONEY = re.compile(r"^\$?\d{1,3}(?:,\d{3})*(?:\.\d{2})-?$")
INT   = re.compile(r"^\d{1,5}$")

def norm_date(s:str)->str:
    for fmt in ("%Y-%m-%d","%m/%d/%Y","%m/%d/%y","%d-%b-%Y"):
        try: return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except: pass
    return ""

def invnum_guess(text:str, fname:str):
    m = re.search(r"\bINV(?:OICE)?\s*#?\s*([A-Z0-9\-]{6,})\b", text, re.I)
    if m: return m.group(1)
    m = re.search(r"\b(\d{6,9}-\d{2})\b", text)
    if m: return m.group(1)
    return os.path.splitext(os.path.basename(fname))[0]

def invdate_guess(text:str, fname:str):
    # in-text
    m = re.search(r"(\d{4}-\d{2}-\d{2})", text);      # 2025-08-28
    if m: return m.group(1)
    m = re.search(r"(\d{2}/\d{2}/\d{2,4})", text);    # 08/28/2025
    if m: return norm_date(m.group(1))
    # filename fallback ..._YYYYMMDD_...
    m = re.search(r"_(20\d{2})(\d{2})(\d{2})_", fname)
    if m: y,mm,dd=m.groups(); return f"{y}-{mm}-{dd}"
    return ""

def page_text_sample(page, max_chars=2000):
    # Get structured text with line breaks; trim to keep prompts small
    t = page.get_text("text")
    return t[:max_chars]

def supplier_guess(text:str):
    # first non-empty uppercase-ish line is a decent guess
    for ln in text.splitlines():
        ln = ln.strip()
        if len(ln) >= 3:
            return ln[:60]
    return ""

# ---------- AI normalizer ----------
AI_SYSTEM = (
"You're an invoice line-item normalizer. "
"Given invoice text (any layout/columns), return ONLY JSON with this exact shape:\n"
"{\"invoice_number\":\"\",\"invoice_date\":\"YYYY-MM-DD\",\"supplier\":\"\","
" \"lines\":[{\"item_code\":\"\",\"item_name\":\"\",\"quantity\":0,\"unit_price\":0.0,\"line_total\":0.0}]}\n"
"- Map any column names (SKU, Part, Code, Description, Qty, QTY, Quantity, Price, Unit, Ext, Amount, Total) into the schema.\n"
"- If description is on the NEXT LINE under the code and that line has no money, use it as item_name.\n"
"- Quantities must be integers; prices/totals are numbers. No currency symbols.\n"
"- Validate math if possible: line_total ~= quantity * unit_price. If unclear, still return best guess.\n"
"- If it's a statement or has no line-items, return lines: [].\n"
"- Never include commentary—ONLY the JSON object."
)

def call_openai_normalize(text_block:str, fname:str):
    """
    Send a compact prompt to the AI to normalize the invoice page to our schema.
    """
    user_prompt = f"""
FILENAME: {fname}
EXTRACTED_TEXT:
\"\"\"
{text_block}
\"\"\"
Return exactly one JSON object with keys: invoice_number, invoice_date, supplier, lines[].
"""
    # Use the Chat Completions API via HTTPX to avoid extra libs
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "gpt-4.1-mini",  # cost-effective; upgrade if needed
        "messages": [
            {"role":"system", "content": AI_SYSTEM},
            {"role":"user", "content": user_prompt}
        ],
        "temperature": 0.1
    }
    try:
        with httpx.Client(timeout=60) as client:
            resp = client.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            # content should be just JSON; be defensive:
            start = content.find("{"); end = content.rfind("}")
            if start >= 0 and end >= 0:
                content = content[start:end+1]
            data = json.loads(content)
            # Hard guard keys
            data.setdefault("invoice_number","")
            data.setdefault("invoice_date","")
            data.setdefault("supplier","")
            data.setdefault("lines",[])
            return data
    except Exception as e:
        return {"invoice_number":"", "invoice_date":"", "supplier":"", "lines":[], "error": str(e)}

# ---------- API ----------
@app.get("/health")
def health(): return {"ok": True}
import os, httpx

@app.get("/check-openai")
def check_openai():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return {"ok": False, "error": "OPENAI_API_KEY not set in environment"}

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "gpt-4.1-mini",
        "messages": [{"role":"user","content":"ping"}],
        "max_tokens": 5,
        "temperature": 0
    }

    try:
        r = httpx.post("https://api.openai.com/v1/chat/completions",
                       headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        txt = r.json()["choices"][0]["message"]["content"]
        return {"ok": True, "echo": txt}
    except Exception as e:
        return {"ok": False, "error": str(e)}

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
                    pdf_bytes = z.read(name)
                    try:
                        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
                    except Exception as e:
                        errors.append(f"{name}: pdf open failed {e}"); continue

                    # --- collect AI-normalized lines across pages ---
                    combined = {"invoice_number":"", "invoice_date":"", "supplier":"", "lines":[]}
                    for p in doc:
                        text_block = page_text_sample(p)
                        ai = call_openai_normalize(text_block, os.path.basename(name))
                        # fill missing header fields progressively
                        if not combined["invoice_number"]: combined["invoice_number"] = ai.get("invoice_number") or ""
                        if not combined["invoice_date"]:   combined["invoice_date"]   = ai.get("invoice_date") or ""
                        if not combined["supplier"]:       combined["supplier"]       = ai.get("supplier") or ""
                        # append lines
                        for ln in ai.get("lines", []):
                            # soft validation + casting
                            try:
                                q  = int(str(ln.get("quantity","0")).replace(",",""))
                            except: q = 0
                            try:
                                up = float(str(ln.get("unit_price","0")).replace(",",""))
                            except: up = 0.0
                            try:
                                lt = float(str(ln.get("line_total","0")).replace(",",""))
                            except: lt = 0.0
                            parsed_lines.append({
                                "supplier": combined["supplier"] or supplier_guess(text_block),
                                "invoice_number": combined["invoice_number"] or invnum_guess(text_block, name),
                                "invoice_date": norm_date(combined["invoice_date"]) or invdate_guess(text_block, name),
                                "item_code": (ln.get("item_code") or "").strip().lower(),
                                "item_name": (ln.get("item_name") or "").strip(),
                                "quantity": q,
                                "unit_price": round(up,2),
                                "line_total": round(lt,2),
                                "file_name": os.path.basename(name)
                            })
                    doc.close()

                    inv_no   = combined["invoice_number"] or invnum_guess("", name)
                    inv_date = norm_date(combined["invoice_date"]) or invdate_guess("", name)
                    supp     = combined["supplier"]

                    if inv_no in seen_invoices:
                        continue
                    seen_invoices.add(inv_no)

                    # compute invoice totals from parsed lines for this invoice
                    inv_lines = [r for r in parsed_lines if r["invoice_number"] == inv_no]
                    if inv_lines:
                        total_qty = sum(r["quantity"] for r in inv_lines)
                        total_val = round(sum(r["line_total"] for r in inv_lines),2)
                        invoices_rows.append({
                            "supplier": supp,
                            "invoice_number": inv_no,
                            "invoice_date": inv_date or None,
                            "lines": total_qty,
                            "total_value": total_val,
                            "file_name": os.path.basename(name)
                        })
        except Exception as e:
            errors.append(f"{f.filename}: zip read failed {e}")

    # Push to Supabase (optional but recommended)
    if sb:
        if invoices_rows:
            sb.table("invoices").upsert(invoices_rows, on_conflict="invoice_number").execute()
        if parsed_lines:
            # avoid accidental dup inserts: optional—could add a unique constraint later
            sb.table("items").insert(parsed_lines).execute()

    # Build quick rollups for UI
    from collections import defaultdict, Counter
    by = defaultdict(lambda: {"qty":0,"val":0,"invs":set(),"names":{}, "first":"","last":""})
    for r in parsed_lines:
        k = r["item_code"]
        by[k]["qty"] += r["quantity"]
        by[k]["val"] += r["line_total"]
        if r["invoice_number"]: by[k]["invs"].add(r["invoice_number"])
        nm=(r["item_name"] or "").strip()
        if nm: by[k]["names"][nm]=by[k]["names"].get(nm,0)+1
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
        best_name = sorted(v["names"].items(), key=lambda t:t[1], reverse=True)[0][0] if v["names"] else ""
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

    monthly_q = Counter(); monthly_v = Counter()
    for r in parsed_lines:
        m = (r["invoice_date"] or "")[:7]
        if len(m)==7:
            monthly_q[m] += r["quantity"]
            monthly_v[m] += r["line_total"]
    months = sorted(monthly_q.keys())
    monthly=[]; prev_q=prev_v=None
    for m in months:
        q=monthly_q[m]; v=round(monthly_v[m],2)
        row={"invoice_month":m,"total_quantity":q,"total_value":v,"qty_mom_pct":None,"val_mom_pct":None}
        if prev_q not in (None,0): row["qty_mom_pct"]=round((q-prev_q)/prev_q,3)
        if prev_v not in (None,0): row["val_mom_pct"]=round((v-prev_v)/prev_v,3)
        monthly.append(row); prev_q,prev_v=q,v

    return JSONResponse({
        "parsed_lines": parsed_lines,
        "master": master,
        "monthly": monthly,
        "oddities": [],   # can add later
        "files_processed": files_count,
        "errors": errors
    })
