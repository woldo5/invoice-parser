import os, io, re, json, zipfile
from datetime import datetime
from typing import List, Optional, Dict, Any

import fitz  # PyMuPDF
import httpx
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse

# ---------- App ----------
app = FastAPI(title="Invoice Parser API")

# ---------- Environment (set these in Render → Environment Variables) ----------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # sk-...
SUPABASE_URL = os.getenv("SUPABASE_URL")      # https://xxxxx.supabase.co
SUPABASE_SERVICE_ROLE = os.getenv("SUPABASE_SERVICE_ROLE")  # long JWT

# Lazily import supabase client only if configured (keeps local tests simple)
_sb = None
def supabase():
    global _sb
    if _sb is None and SUPABASE_URL and SUPABASE_SERVICE_ROLE:
        from supabase import create_client, Client
        _sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE)
    return _sb

# ---------- Helpers ----------
def norm_date(s: str) -> str:
    if not s: return ""
    for fmt in ("%Y-%m-%d","%m/%d/%Y","%m/%d/%y","%d-%b-%Y"):
        try: return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except: pass
    return ""

def invnum_fallback(fname: str) -> str:
    base = os.path.basename(fname)
    return os.path.splitext(base)[0]

def invdate_from_fname(fname: str) -> str:
    m = re.search(r"_(20\d{2})(\d{2})(\d{2})_", fname)
    if m:
        y, mm, dd = m.groups()
        return f"{y}-{mm}-{dd}"
    return ""

def page_text_sample(page, max_chars=2000) -> str:
    # Keep prompts compact; AI sees enough to map columns
    return page.get_text("text")[:max_chars]

AI_SYSTEM = (
    "You're an invoice line-item normalizer. "
    "Given messy invoice text (any layout/column names), return ONLY a JSON object:\n"
    "{\"invoice_number\":\"\",\"invoice_date\":\"YYYY-MM-DD\",\"supplier\":\"\","
    "\"lines\":[{\"item_code\":\"\",\"item_name\":\"\",\"quantity\":0,\"unit_price\":0.0,\"line_total\":0.0}]}\n"
    "- Map any headers: (SKU/Part/Code), (Description), (Qty/Quantity/QTY), (Unit/Price), (Ext/Amount/Total).\n"
    "- If description sits UNDER the code on the next line (and has no money), use it as item_name.\n"
    "- Quantities are integers. Prices/totals are numbers; strip currency symbols.\n"
    "- If it's a statement or no items, return lines: []. No commentary. Only JSON."
)

async def ai_normalize(block: str, fname: str) -> Dict[str, Any]:
    """
    Calls OpenAI to normalize a page of invoice text to our schema.
    Returns a dict with keys: invoice_number, invoice_date, supplier, lines[].
    """
    if not OPENAI_API_KEY:
        return {"invoice_number":"", "invoice_date":"", "supplier":"", "lines":[], "error":"OPENAI_API_KEY not set"}

    user_prompt = f"""
FILENAME: {fname}
EXTRACTED_TEXT:
\"\"\"
{block}
\"\"\"
Return exactly one JSON object with keys: invoice_number, invoice_date, supplier, lines[].
"""

    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": "gpt-4.1-mini",
        "messages": [
            {"role":"system","content": AI_SYSTEM},
            {"role":"user","content": user_prompt}
        ],
        "temperature": 0.1,
        "max_tokens": 1200
    }
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"]
        # Defend against extra text
        start = content.find("{"); end = content.rfind("}")
        if start >= 0 and end >= 0:
            content = content[start:end+1]
        try:
            data = json.loads(content)
        except Exception:
            data = {"invoice_number":"", "invoice_date":"", "supplier":"", "lines":[]}
        data.setdefault("invoice_number","")
        data.setdefault("invoice_date","")
        data.setdefault("supplier","")
        data.setdefault("lines",[])
        return data

def months_between(a: str, b: str) -> int:
    if not a or not b: return 1
    ay, am = map(int, a.split("-")[:2]); by, bm = map(int, b.split("-")[:2])
    return max(1, (by - ay)*12 + (bm - am) + 1)

def sb_upsert_chunked(table: str, rows: List[Dict[str, Any]], chunk: int = 500, on_conflict: Optional[str] = None):
    client = supabase()
    if not client or not rows: return []
    out = []
    for i in range(0, len(rows), chunk):
        batch = rows[i:i+chunk]
        q = client.table(table).upsert(batch)
        if on_conflict:
            q = q.on_conflict(on_conflict)
        out.append(q.execute())
    return out

def sb_insert_chunked(table: str, rows: List[Dict[str, Any]], chunk: int = 1000):
    client = supabase()
    if not client or not rows: return []
    out = []
    for i in range(0, len(rows), chunk):
        batch = rows[i:i+chunk]
        out.append(client.table(table).insert(batch).execute())
    return out

# ---------- Routes ----------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/check-openai")
def check_openai():
    # A simple live test that actually hits OpenAI
    key = OPENAI_API_KEY
    if not key:
        return {"ok": False, "error": "OPENAI_API_KEY not set in environment"}
    try:
        r = httpx.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={"model":"gpt-4.1-mini","messages":[{"role":"user","content":"ping"}],"max_tokens":5},
            timeout=20,
        )
        r.raise_for_status()
        txt = r.json()["choices"][0]["message"]["content"]
        return {"ok": True, "echo": txt}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/ingest")
async def ingest(files: List[UploadFile] = File(...)):
    """
    Accepts one or more ZIP files (field name 'files'), extracts PDFs, uses AI to normalize lines.
    - De-dupes by invoice_number.
    - Returns rollups for UI.
    - If Supabase env vars are set, upserts into 'invoices' and 'items'.
    """
    parsed_lines: List[Dict[str, Any]] = []
    invoices_rows: List[Dict[str, Any]] = []
    errors: List[str] = []
    seen_invoices = set()
    files_processed = 0

    for f in files:
        try:
            blob = await f.read()
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                for name in z.namelist():
                    if not name.lower().endswith(".pdf"):
                        continue
                    files_processed += 1
                    pdf_bytes = z.read(name)
                    try:
                        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
                    except Exception as e:
                        errors.append(f"{name}: pdf open failed {e}")
                        continue

                    combined = {"invoice_number":"", "invoice_date":"", "supplier":"", "lines":[]}
                    # Run per-page normalization and merge
                    for p in doc:
                        block = page_text_sample(p)
                        ai = await ai_normalize(block, os.path.basename(name))
                        if not combined["invoice_number"]:
                            combined["invoice_number"] = (ai.get("invoice_number") or "").strip() or invnum_fallback(name)
                        if not combined["invoice_date"]:
                            combined["invoice_date"] = norm_date(ai.get("invoice_date","")) or invdate_from_fname(name)
                        if not combined["supplier"]:
                            combined["supplier"] = (ai.get("supplier") or "").strip()

                        for ln in ai.get("lines", []):
                            # Safe casting & cleanup
                            try: q = int(str(ln.get("quantity","0")).replace(",",""))
                            except: q = 0
                            try: up = float(str(ln.get("unit_price","0")).replace(",",""))
                            except: up = 0.0
                            try: lt = float(str(ln.get("line_total","0")).replace(",",""))
                            except: lt = 0.0

                            parsed_lines.append({
                                "supplier": combined["supplier"],
                                "invoice_number": combined["invoice_number"],
                                "invoice_date": combined["invoice_date"],
                                "item_code": (ln.get("item_code") or "").strip().lower(),
                                "item_name": (ln.get("item_name") or "").strip(),
                                "quantity": q,
                                "unit_price": round(up,2),
                                "line_total": round(lt,2),
                                "file_name": os.path.basename(name),
                            })
                    doc.close()

                    inv_no   = combined["invoice_number"] or invnum_fallback(name)
                    inv_date = combined["invoice_date"] or invdate_from_fname(name)
                    supp     = combined["supplier"]

                    if inv_no in seen_invoices:
                        continue
                    seen_invoices.add(inv_no)

                    # Compute totals from parsed_lines for this invoice
                    inv_rows = [r for r in parsed_lines if r["invoice_number"] == inv_no]
                    if inv_rows:
                        total_qty = sum(r["quantity"] for r in inv_rows)
                        total_val = round(sum(r["line_total"] for r in inv_rows), 2)
                        invoices_rows.append({
                            "supplier": supp,
                            "invoice_number": inv_no,
                            "invoice_date": inv_date or None,
                            "lines": total_qty,
                            "total_value": total_val,
                            "file_name": os.path.basename(name),
                        })
        except zipfile.BadZipFile:
            errors.append(f"{getattr(f,'filename','(upload)')}: not a zip file")
        except Exception as e:
            errors.append(f"{getattr(f,'filename','(upload)')}: zip read failed {e}")

    # --- Optional: write to Supabase if configured ---
    inv_count = len(invoices_rows)
    item_count = len(parsed_lines)
    if inv_count or item_count:
        if supabase():
            sb_upsert_chunked("invoices", invoices_rows, chunk=500, on_conflict="invoice_number")
            sb_insert_chunked("items", parsed_lines, chunk=1000)

    # --- Build rollups for immediate UI use ---
    from collections import defaultdict, Counter
    by = defaultdict(lambda: {"qty":0,"val":0,"invs":set(),"names":{}, "first":"","last":""})
    for r in parsed_lines:
        k = r["item_code"]
        by[k]["qty"] += r["quantity"]
        by[k]["val"] += r["line_total"]
        if r["invoice_number"]: by[k]["invs"].add(r["invoice_number"])
        nm = (r["item_name"] or "").strip()
        if nm: by[k]["names"][nm] = by[k]["names"].get(nm, 0) + 1
        d = r["invoice_date"] or ""
        if d:
            by[k]["first"] = min(filter(None,[by[k]["first"], d])) if by[k]["first"] else d
            by[k]["last"]  = max(filter(None,[by[k]["last"], d]))  if by[k]["last"]  else d

    master = []
    for k, v in by.items():
        best_name = sorted(v["names"].items(), key=lambda t: t[1], reverse=True)[0][0] if v["names"] else ""
        invs = len(v["invs"]); span = months_between(v["first"], v["last"])
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
        if len(m) == 7:
            monthly_q[m] += r["quantity"]
            monthly_v[m] += r["line_total"]
    months = sorted(monthly_q.keys())
    monthly = []; prev_q = prev_v = None
    for m in months:
        q = monthly_q[m]; v = round(monthly_v[m], 2)
        row = {"invoice_month": m, "total_quantity": q, "total_value": v, "qty_mom_pct": None, "val_mom_pct": None}
        if prev_q not in (None, 0): row["qty_mom_pct"] = round((q - prev_q)/prev_q, 3)
        if prev_v not in (None, 0): row["val_mom_pct"] = round((v - prev_v)/prev_v, 3)
        monthly.append(row); prev_q, prev_v = q, v

    return JSONResponse({
        "files_processed": files_processed,
        "parsed_lines": parsed_lines,
        "master": master,
        "monthly": monthly,
        "oddities": [],   # add price/qty anomaly flags later if you want
        "supabase": {
            "configured": bool(supabase()),
            "invoices_rows": inv_count,
            "items_rows": item_count
        },
        "errors": errors
    })
