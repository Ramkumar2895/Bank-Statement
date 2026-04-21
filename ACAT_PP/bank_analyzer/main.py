from fastapi import FastAPI, UploadFile, File, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from typing import Optional
import os
import io
import logging
import uuid
from datetime import datetime, timedelta, timezone
from threading import Lock

# Indian Standard Time (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))

from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler

from pdf_parser import check_pdf_encrypted, extract_text_from_pdf, parse_transactions
from categorizer import categorize_transactions, get_category_summary, get_monthly_summary, ALL_CATEGORIES, LEND_CATEGORIES, reload_learned_categories
from database import (
    save_transactions, seed_passwords, get_bank_password, get_all_bank_names,
    get_saved_categories, get_all_transactions,
)
from email_fetcher import fetch_hdfc_alerts, fetch_hdfc_balance

# Load .env for Gmail credentials (for local dev)
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
GMAIL_USER = os.getenv("GMAIL_USER", "").strip()
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "").strip()

logger = logging.getLogger("bank_analyzer")
logging.basicConfig(level=logging.INFO)

# Debug: Log if env vars are loaded
logger.info("GMAIL_USER env loaded: %s", "YES" if GMAIL_USER else "NO (empty)")
logger.info("GMAIL_APP_PASSWORD env loaded: %s", "YES (hidden)" if GMAIL_APP_PASSWORD else "NO (empty)")

app = FastAPI(title="Bank Statement Analyzer")

# ---------------------------------------------------------------------------
# Background email auto-fetch with pending approval queue
# ---------------------------------------------------------------------------
scheduler = BackgroundScheduler()
_pending_lock = Lock()
_pending_transactions: list[dict] = []      # txns waiting for user approval
_existing_keys: set[tuple] | None = None    # cache of saved txn keys
_auto_fetch_log: list[dict] = []

CHECK_INTERVAL_SECONDS = 30  # check every 30s (Gmail rate-limits at <10s)

# Available balance from HDFC morning email
_balance_info: dict | None = None  # {balance, date, timestamp}


def _get_existing_keys() -> set[tuple]:
    """Load all existing transaction keys to detect duplicates."""
    global _existing_keys
    if _existing_keys is None:
        _existing_keys = set()
        for t in get_all_transactions():
            _existing_keys.add((
                str(t["date"]).strip(),
                str(t["description"]).strip().lower(),
                round(float(t["debit"]), 2),
            ))
    return _existing_keys


def _invalidate_keys():
    global _existing_keys
    _existing_keys = None


def _auto_fetch_job():
    """Background job: fetch today's emails → put NEW ones in pending queue for approval."""
    global _balance_info
    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        return

    # Fetch today's available balance from morning email
    try:
        bal = fetch_hdfc_balance(GMAIL_USER, GMAIL_APP_PASSWORD)
        if bal:
            _balance_info = bal
            logger.info("Balance updated: ₹%.2f as of %s", bal["balance"], bal["date"])
    except Exception as e:
        logger.error("Balance fetch error: %s", e)

    try:
        raw = fetch_hdfc_alerts(GMAIL_USER, GMAIL_APP_PASSWORD, days_back=0)
        entry = {"time": datetime.now(IST).strftime("%H:%M:%S"), "fetched": len(raw), "new": 0, "error": None}

        if raw:
            txns = categorize_transactions(raw)
            saved_cats = get_saved_categories()
            existing = _get_existing_keys()

            # Also check what's already pending
            with _pending_lock:
                pending_keys = {(t["date"], t["description"].lower(), round(float(t["debit"]), 2)) for t in _pending_transactions}

            new_count = 0
            for t in txns:
                key = (str(t["date"]).strip(), str(t["description"]).strip().lower(), round(float(t["debit"]), 2))
                if key in existing or key in pending_keys:
                    continue  # already saved or already pending
                if saved_cats and key in saved_cats:
                    t["category"] = saved_cats[key]
                t["_pending_id"] = str(uuid.uuid4())
                with _pending_lock:
                    _pending_transactions.append(t)
                new_count += 1

            entry["new"] = new_count

        _auto_fetch_log.append(entry)
        if len(_auto_fetch_log) > 100:
            _auto_fetch_log.pop(0)

        if entry["new"] > 0:
            logger.info("Auto-fetch: %d new transactions pending approval", entry["new"])
    except Exception as e:
        _auto_fetch_log.append({"time": datetime.now(IST).strftime("%H:%M:%S"), "fetched": 0, "new": 0, "error": str(e)})
        logger.error("Auto-fetch error: %s", e)


def _start_scheduler():
    """Start the background email check scheduler."""
    if scheduler.get_job("email_auto_fetch"):
        scheduler.remove_job("email_auto_fetch")
    scheduler.add_job(
        _auto_fetch_job, "interval", seconds=CHECK_INTERVAL_SECONDS,
        id="email_auto_fetch", replace_existing=True,
    )
    if not scheduler.running:
        scheduler.start()
    logger.info("Email auto-fetch started (every %ds)", CHECK_INTERVAL_SECONDS)


@app.on_event("startup")
def on_startup():
    """Seed passwords and auto-start email fetcher using .env credentials."""
    seed_passwords()
    if GMAIL_USER and GMAIL_APP_PASSWORD:
        logger.info("Gmail config found in .env — starting auto-fetch for %s", GMAIL_USER)
        _start_scheduler()
        # Run first check immediately
        _auto_fetch_job()
    else:
        logger.warning("GMAIL_USER / GMAIL_APP_PASSWORD not set in .env — email auto-fetch disabled")

# Setup templates
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates_dir = os.path.join(BASE_DIR, "templates")
static_dir = os.path.join(BASE_DIR, "static")

os.makedirs(templates_dir, exist_ok=True)
os.makedirs(static_dir, exist_ok=True)

templates = Jinja2Templates(directory=templates_dir)
app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/check-password")
async def check_password(file: UploadFile = File(...)):
    """Check if uploaded PDF is password-protected."""
    file_bytes = await file.read()
    is_encrypted = check_pdf_encrypted(file_bytes)
    return JSONResponse({"encrypted": is_encrypted})


@app.get("/banks")
async def list_banks():
    """Return list of banks with stored passwords."""
    return JSONResponse({"banks": get_all_bank_names()})


@app.get("/dashboard")
async def dashboard_data():
    """Return all saved transactions with summaries for the dashboard."""
    transactions = get_all_transactions()
    if not transactions:
        return JSONResponse({"transactions": [], "category_summary": {}, "monthly_summary": {}, "total_income": 0, "total_expense": 0, "net_savings": 0, "total_lend": 0, "total_transactions": 0, "all_categories": ALL_CATEGORIES, "lend_categories": list(LEND_CATEGORIES)})

    category_summary = get_category_summary(transactions)
    monthly_summary = get_monthly_summary(transactions)

    total_income = sum(t["credit"] for t in transactions)
    total_expense = sum(t["debit"] for t in transactions)
    total_lend = sum(t["debit"] for t in transactions if t.get("category") in LEND_CATEGORIES)

    return JSONResponse({
        "transactions": transactions,
        "category_summary": category_summary,
        "monthly_summary": monthly_summary,
        "total_income": round(total_income, 2),
        "total_expense": round(total_expense, 2),
        "net_savings": round(total_income - total_expense, 2),
        "total_lend": round(total_lend, 2),
        "total_transactions": len(transactions),
        "all_categories": ALL_CATEGORIES,
        "lend_categories": list(LEND_CATEGORIES),
    })


@app.post("/analyze")
async def analyze_statement(
    file: UploadFile = File(...),
    password: Optional[str] = Form(None),
    bank: Optional[str] = Form(None),
):
    """Parse and analyze the bank statement."""
    file_bytes = await file.read()

    # If a bank is selected, auto-fetch stored password
    if check_pdf_encrypted(file_bytes) and not password and bank:
        stored_pw = get_bank_password(bank)
        if stored_pw:
            password = stored_pw

    # Check if encrypted
    if check_pdf_encrypted(file_bytes) and not password:
        return JSONResponse(
            {"error": "password_required", "message": "This PDF is password-protected. Please enter your password."},
            status_code=400,
        )

    try:
        text = extract_text_from_pdf(file_bytes, password)
    except ValueError as e:
        return JSONResponse({"error": "parse_error", "message": str(e)}, status_code=400)

    if not text.strip():
        return JSONResponse({"error": "empty", "message": "Could not extract any text from the PDF."}, status_code=400)

    transactions = parse_transactions(text, file_bytes=file_bytes, password=password)
    if not transactions:
        return JSONResponse(
            {"error": "no_transactions", "message": "No transactions found. The format may not be supported yet.", "raw_text_preview": text[:500]},
            status_code=400,
        )

    transactions = categorize_transactions(transactions)

    # Apply saved categories from previous uploads (preserve user edits)
    saved_cats = get_saved_categories()
    if saved_cats:
        for t in transactions:
            key = (
                str(t["date"]).strip(),
                str(t["description"]).strip().lower(),
                round(float(t["debit"]), 2),
            )
            if key in saved_cats:
                t["category"] = saved_cats[key]

    category_summary = get_category_summary(transactions)
    monthly_summary = get_monthly_summary(transactions)

    total_income = sum(t["credit"] for t in transactions)
    total_expense = sum(t["debit"] for t in transactions)
    total_lend = sum(t["debit"] for t in transactions if t.get("category") in LEND_CATEGORIES)

    return JSONResponse({
        "success": True,
        "total_transactions": len(transactions),
        "total_income": round(total_income, 2),
        "total_expense": round(total_expense, 2),
        "net_savings": round(total_income - total_expense, 2),
        "total_lend": round(total_lend, 2),
        "transactions": transactions,
        "category_summary": category_summary,
        "monthly_summary": monthly_summary,
        "all_categories": ALL_CATEGORIES,
        "lend_categories": list(LEND_CATEGORIES),
    })


@app.post("/debug")
async def debug_pdf(
    file: UploadFile = File(...),
    password: Optional[str] = Form(None),
):
    """Return raw extracted text and table data from the PDF for debugging."""
    file_bytes = await file.read()
    try:
        text = extract_text_from_pdf(file_bytes, password)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)

    lines = [l for l in text.split('\n') if l.strip()]

    # Also extract tables via pdfplumber
    table_data = []
    try:
        import pdfplumber
        pdf_io = io.BytesIO(file_bytes)
        with pdfplumber.open(pdf_io, password=password or "") as pdf:
            for i, page in enumerate(pdf.pages):
                tables = page.extract_tables()
                for t_idx, table in enumerate(tables):
                    table_data.append({
                        "page": i + 1,
                        "table_index": t_idx,
                        "rows": [[str(c) if c else "" for c in row] for row in table[:20]],
                        "total_rows": len(table),
                    })
    except Exception as e:
        table_data = [{"error": str(e)}]

    return JSONResponse({
        "total_chars": len(text),
        "total_lines": len(lines),
        "first_50_lines": lines[:50],
        "raw_text_preview": text[:3000],
        "tables": table_data,
    })


@app.post("/save")
async def save_to_db(request: Request):
    """Save transactions to MongoDB Atlas."""
    body = await request.json()
    transactions = body.get("transactions", [])
    filename = body.get("filename", "unknown")

    if not transactions:
        return JSONResponse({"error": "no_data", "message": "No transactions to save."}, status_code=400)

    try:
        result = save_transactions(transactions, filename)
        reload_learned_categories()  # Refresh learned categories for next upload
        return JSONResponse({
            "success": True,
            "statement_id": result["statement_id"],
            "total_saved": result["total_saved"],
            "skipped": result["skipped"],
            "updated": result["updated"],
        })
    except Exception as e:
        return JSONResponse({"error": "db_error", "message": str(e)}, status_code=500)


@app.post("/fetch-emails")
async def fetch_emails(request: Request):
    """Manual one-time fetch using .env credentials."""
    body = await request.json()
    days_back = body.get("days_back", 7)

    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        return JSONResponse({"error": "missing_credentials", "message": "GMAIL_USER / GMAIL_APP_PASSWORD not set in .env."}, status_code=400)

    try:
        raw_transactions = fetch_hdfc_alerts(GMAIL_USER, GMAIL_APP_PASSWORD, days_back=days_back)
    except ValueError as e:
        return JSONResponse({"error": "login_error", "message": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": "fetch_error", "message": f"Failed to fetch emails: {str(e)}"}, status_code=500)

    if not raw_transactions:
        return JSONResponse({"success": True, "message": f"No HDFC InstaAlert emails found in the last {days_back} days.", "total_fetched": 0, "total_saved": 0, "skipped": 0})

    transactions = categorize_transactions(raw_transactions)
    saved_cats = get_saved_categories()
    if saved_cats:
        for t in transactions:
            key = (str(t["date"]).strip(), str(t["description"]).strip().lower(), round(float(t["debit"]), 2))
            if key in saved_cats:
                t["category"] = saved_cats[key]

    try:
        result = save_transactions(transactions, f"email_alerts_{days_back}d")
        reload_learned_categories()
        _invalidate_keys()
    except Exception as e:
        return JSONResponse({"error": "save_error", "message": str(e)}, status_code=500)

    return JSONResponse({"success": True, "total_fetched": len(raw_transactions), "total_saved": result["total_saved"], "skipped": result["skipped"], "updated": result.get("updated", 0)})


# ---------------------------------------------------------------------------
# Gmail auto-fetch status endpoint
# ---------------------------------------------------------------------------

@app.get("/gmail-config/status")
async def gmail_status():
    """Return whether Gmail is configured (via .env) and auto-fetch is running."""
    configured = bool(GMAIL_USER and GMAIL_APP_PASSWORD)
    running = scheduler.get_job("email_auto_fetch") is not None
    pending_count = len(_pending_transactions)
    return JSONResponse({
        "configured": configured,
        "gmail_user": GMAIL_USER if configured else None,
        "running": running,
        "pending_count": pending_count,
        "log": _auto_fetch_log[-5:],
    })


@app.get("/balance")
async def get_balance():
    """Return current available balance from HDFC morning email, adjusted by today's transactions."""
    logger.info("get_balance() called. _balance_info: %s", "SET" if _balance_info else "EMPTY")
    if not _balance_info:
        logger.warning("Balance not available - email fetch may have failed")
        return JSONResponse({"available": False})

    # Start with morning balance
    balance = _balance_info["balance"]

    # Subtract today's already-saved transactions
    today_str = datetime.now(IST).strftime("%d/%m/%y")
    for t in get_all_transactions():
        if str(t.get("date", "")).strip() == today_str:
            balance -= float(t.get("debit", 0))
            balance += float(t.get("credit", 0))

    # Also adjust by pending (not-yet-saved) transactions
    with _pending_lock:
        for t in _pending_transactions:
            balance -= t.get("debit", 0)
            balance += t.get("credit", 0)

    logger.info("Returning balance: ₹%.2f", balance)
    return JSONResponse({
        "available": True,
        "balance": round(balance, 2),
        "morning_balance": _balance_info["balance"],
        "as_of": _balance_info["date"],
        "updated_at": _balance_info["timestamp"],
    })


@app.get("/pending-transactions")
async def get_pending():
    """Return transactions waiting for user approval."""
    with _pending_lock:
        return JSONResponse({"transactions": list(_pending_transactions)})


@app.post("/approve-transaction")
async def approve_txn(request: Request):
    """Approve a pending transaction (optionally with changed category) → save to DB."""
    body = await request.json()
    pending_id = body.get("pending_id")
    new_category = body.get("category")

    with _pending_lock:
        txn = None
        for i, t in enumerate(_pending_transactions):
            if t.get("_pending_id") == pending_id:
                txn = _pending_transactions.pop(i)
                break

    if not txn:
        return JSONResponse({"error": "not_found", "message": "Transaction not found or already processed."}, status_code=404)

    if new_category:
        txn["category"] = new_category

    # Remove internal pending ID before saving
    txn.pop("_pending_id", None)

    try:
        result = save_transactions([txn], "email_approved")
        reload_learned_categories()
        _invalidate_keys()
    except Exception as e:
        return JSONResponse({"error": "save_error", "message": str(e)}, status_code=500)

    return JSONResponse({"success": True, "saved": result["total_saved"]})


@app.post("/approve-all")
async def approve_all(request: Request):
    """Approve all pending transactions at once."""
    body = await request.json()
    updates = body.get("updates", {})  # {pending_id: new_category} for changed categories

    with _pending_lock:
        txns = list(_pending_transactions)
        _pending_transactions.clear()

    for t in txns:
        pid = t.get("_pending_id", "")
        if pid in updates and updates[pid]:
            t["category"] = updates[pid]
        t.pop("_pending_id", None)

    if not txns:
        return JSONResponse({"success": True, "saved": 0})

    try:
        result = save_transactions(txns, "email_approved_bulk")
        reload_learned_categories()
        _invalidate_keys()
    except Exception as e:
        return JSONResponse({"error": "save_error", "message": str(e)}, status_code=500)

    return JSONResponse({"success": True, "saved": result["total_saved"]})


@app.post("/dismiss-transaction")
async def dismiss_txn(request: Request):
    """Dismiss (skip) a pending transaction without saving."""
    body = await request.json()
    pending_id = body.get("pending_id")

    with _pending_lock:
        for i, t in enumerate(_pending_transactions):
            if t.get("_pending_id") == pending_id:
                _pending_transactions.pop(i)
                return JSONResponse({"success": True})

    return JSONResponse({"error": "not_found"}, status_code=404)


@app.on_event("shutdown")
def on_shutdown():
    if scheduler.running:
        scheduler.shutdown(wait=False)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
