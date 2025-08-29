import io, zipfile, re, os, json, time
from datetime import datetime
from typing import List

import httpx
import fitz  # PyMuPDF
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse

# ---------- CONFIG ----------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Supabase is optional: only used if env vars are present
try:
    from supabase import create_client, Client
    sb = create_client(SUPABASE_URL, SUPABASE_KEY) if (SUPABASE_URL and SUPABASE_KEY) else None
except Exception:
    sb = None

app = FastAPI()

# ---------- Helpers: parsing + normalization ----------
MONEY = re.compile(r"^\$?\d{1,3}(?:,\d{3})*(?:\.\d{2})-?$")
INT   = re.compile(r"^\d{1,5}$")

def norm_date(s: str) -> str:
    if not s: return ""
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%d-%b-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except: 
            pass
    return ""

def invnum_guess(text: str, fname: str) -> str:
    m = re.search(r"\bINV(?:OICE)?\s*#?\s*([A-Z0-9\-]{6,})\b", text, re.I)
    if m: return m.group(1)
    m = re.search(r"\b(\d{6,9}-\d{2})\b", text)
    if m: return m.group(1)
    return os.path.splitext(os.path.basename(fname))[0]

def invdate_guess(text: str, fname: str) -> str:
    m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if m: return m.group(1)
    m = re.search(r"(\d{2}/\d{2}/\d{2,4})", text)
    if m: return norm_date(m.group(1))
    m = re.search(r"_(20\d{2})(\d{2})(\d{2})_", fname)
    if m:
        y, mm, dd = m.groups()
        return f"{y}-{mm}-{dd}"
    return ""

def supplier_guess(text: str) -> str:
    for ln in text.splitlines():
        ln = ln.strip()
        if len(ln) >= 3:
            return ln[:60]
    return ""

def doc_text_sample(doc, max_chars=4000) -> str:
    """Concatenate trimmed text across pages -> one AI call per PDF."""
    parts = []
    total = 0
    for p in doc:
        t = p.get_text("text") or ""
        if not t: 
            continue
        chunk = t[:800]  # up to 800 chars per page
        parts.append(chunk)
        total += len(chunk)
        if total >= max_chars:
            break
    return ("\n---PAGE BREAK---\n".join(parts))[:max_chars]

# ---------- AI Normalizer (with rate limit protection) ----------
AI_SYSTEM = (
    "You're an invoice line-item normalizer. "
    "Given invoice text of any layout, return ONLY JSON:\n"
    "{\"invoice_number\":\"\",\"invoice_date\":\"YYYY-MM-DD\",\"supplier\":\"\","
    "\"lines\":[{\"item_code\":\"\",\"item_name\":\"\",\"quantity\":0,\"unit_price\":0.0,\"line_total\":0.0}]}\n"
    "- Map any column names (SKU/Part/Code, Description, Qty, Unit/Price, Ext/Amount/Total) to this schema.\n"
    "- If description is on the next line under the code with no money tokens, use it as item_name.\n"
    "- Quantities are integers; prices/totals are numbers (no currency symbols).\n"
    "- If it's a statement or has no items, return lines: [].\n"
    "- Never include commentary—ONLY the JSON object."
)

_last_call = 0.0
def _throttle(min_interval=0.9):
    """Simple client-side rate limiter to avoid bursts."""
    global _last_call
    now = time.time()
    wait = _last_call + min_interval - now
    if wait > 0:
        time.sleep(wait)
    _last_call = time.time()

def call_openai_normalize(text_block: str, fname: str, max_retries=5) -> dict:
    """One AI call per PDF, with retry/backoff and Retry-After support."""
    user_prompt = f"""
FILENAME: {fname}
EXTRACTED_TEXT:
\"\"\"
{text_block}
\"\"\"
Return exactly one JSON object with keys: invoice_number, invoice_date, supplier, lines[].
"""
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "gpt-4o-mini",  # fast/cost-effective; switch to gpt-4.1-mini if desired
        "messages": [
            {"role": "system", "content": AI_SYSTEM},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0.1
    }

    delay = 2.0
    for attempt in range(max_retries):
        try:
            _throttle(0.9)  # prevent bursty calls
            with httpx.Client(timeout=60) as client:
                resp = client.post("https://api.openai.com/v1/chat/completions",
                                   headers=headers, json=payload)
                if resp.status_code == 429:
                    ra = resp.headers.get("Retry-After")
                    wait_s = float(ra) if ra else delay
                    time.sleep(wait_s)
                    delay = min(delay * 2, 30)  # exponential backoff capped
                    continue
                resp.raise_for_status()
                content = resp.json()["choices"][0]["message"]["content"]
                start = content.find("{"); end = content.rfind("}")
                content = content[start:end+1] if (start >= 0 and end >= 0) else "{}"
                data = json.loads(content)
                data.setdefault("invoice_number","")
                data.setdefault("invoice_date","")
                data.setdefault("supplier","")
                data.setdefault("lines",[])
                return data
        except Exception as e:
            if attempt == max_retries - 1:
                return {"invoice_number":"", "invoice_date":"", "supplier":"", "lines":[], "error": f"{type(e).__name__}: {e}"}
            time.sleep(delay)
            delay = min(delay * 2, 30)
    return {"invoice_number":"", "invoice_date":"", "supplier":"", "lines":[]}

# ---------- Supabase helpers: chunked writes ----------
def sb_upsert_chunked(table: str, rows: List[dict], chunk=500, on_conflict=None):
    if not (sb and rows): 
        return
    for i in range(0, len(rows), chunk):
        batch = rows[i:i+chunk]
        q = sb.table(table).upsert(batch)
        if on_conflict:
            q = q.on_conflict(on_conflict)
        q.execute()

def sb_insert_chunked(table: str, rows: List[dict], chunk=1000):
    if not (sb and rows):
        return
    for i in range(0, len(rows), chunk):
        batch = rows[i:i+chunk]
        sb.table(table).insert(batch).execute()

# ---------- API ----------
@app.get("/health")
def health():
    return {"ok": True, "openai": bool(OPENAI_API_KEY), "supabase": bool(sb)}

@app.post("/ingest")
async def ingest(files: List[UploadFile] = File(...)):
    parsed_lines = []
    invoices_rows = []
    errors = []
    seen_invoices = set()
    files_count = 0

    for f in files:
        try:
            blob = await f.read()
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                for name in z.namelist():
                    if not name.lower().endswith(".pdf"):
                        continue
                    files_count += 1
                    pdf_bytes = z.read(name)
                    try:
                        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
                    except Exception as e:
                        errors.append(f"{name}: pdf open failed {e}")
                        continue

                    # --- ONE AI CALL PER PDF ---
                    text_block = doc_text_sample(doc)
                    ai = {}
                    if OPENAI_API_KEY:
                        ai = call_openai_normalize(text_block, os.path.basename(name))
                    # fallback if AI not set or returns no lines
                    if not ai or not ai.get("lines"):
                        ai = {
                            "invoice_number": invnum_guess(text_block, name),
                            "invoice_date": invdate_guess(text_block, name),
                            "supplier": supplier_guess(text_block),
                            "lines": []
                        }

                    inv_no   = ai.get("invoice_number") or invnum_guess(text_block, name)
                    inv_date = norm_date(ai.get("invoice_date") or "") or invdate_guess(text_block, name)
                    supp     = ai.get("supplier") or supplier_guess(text_block)

                    if inv_no in seen_invoices:
                        doc.close()
                        continue
                    seen_invoices.add(inv_no)

                    # Build parsed_lines from AI lines
                    total_qty = 0
                    total_val = 0.0
                    for ln in ai.get("lines", []):
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
                            "supplier": supp,
                            "invoice_number": inv_no,
                            "invoice_date": inv_date,
                            "item_code": (ln.get("item_code") or "").strip().lower(),
                            "item_name": (ln.get("item_name") or "").strip(),
                            "quantity": q,
                            "unit_price": round(up, 2),
                            "line_total": round(lt, 2),
                            "file_name": os.path.basename(name)
                        })
                        total_qty += q
                        total_val += lt

                    # Only add invoice row if there are any lines or totals
                    if total_qty > 0 or total_val > 0:
                        invoices_rows.append({
                            "supplier": supp,
                            "invoice_number": inv_no,
                            "invoice_date": inv_date or None,
                            "lines": total_qty,
                            "total_value": round(total_val, 2),
                            "file_name": os.path.basename(name)
                        })

                    doc.close()
        except zipfile.BadZipFile:
            errors.append(f"{f.filename}: not a valid zip file")
        except Exception as e:
            errors.append(f"{f.filename}: zip read failed {e}")

    # ---- Write to Supabase (optional) ----
    # invoices: upsert on unique invoice_number
    sb_upsert_chunked("invoices", invoices_rows, chunk=500, on_conflict="invoice_number")
    # items: insert in chunks
    sb_insert_chunked("items", parsed_lines, chunk=1000)

    # ---- Build light rollups for immediate UI use ----
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
        ay,am = map(int,a.split("-")[:2]); by_,bm = map(int,b.split("-")[:2])
        return max(1,(by_ - ay)*12 + (bm - am) + 1)

    master=[]
    for k,v in by.items():
        best_name = sorted(v["names"].items(), key=lambda t:t[1], reverse=True)[0][0] if v["names"] else ""
        invs=len(v["invs"]); span=months_between(v["first"], v["last"])
        master.append({
            "item_code": k,
            "item_name": best_name,
            "invoices": invs,
            "frequency_per_month": round(invs/span, 2),
            "total_quantity": v["qty"],
            "total_value": round(v["val"], 2),
            "avg_price": round(v["val"]/v["qty"], 2) if v["qty"] else 0.0,
            "last_invoice_date": v["last"] or ""
        })
    master.sort(key=lambda r: r["total_value"], reverse=True)

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
        "oddities": [],   # add rules later if you want
        "files_processed": files_count,
        "errors": errors,
        "supabase": {
            "invoices_rows": len(invoices_rows),
            "items_rows": len(parsed_lines),
            "enabled": bool(sb)
        }
    })
