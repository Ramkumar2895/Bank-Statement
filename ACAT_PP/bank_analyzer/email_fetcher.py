"""
Fetch HDFC Bank InstaAlert emails from Gmail and parse transactions.

HDFC InstaAlert emails contain:
  - Debit alerts: amount debited, to whom, balance
  - Credit alerts: amount credited, from whom, balance

Requires Gmail App Password (not regular password).
Steps to get App Password:
  1. Go to https://myaccount.google.com/security
  2. Enable 2-Step Verification
  3. Go to App passwords -> Generate for "Mail"
  4. Use that 16-char password here
"""

import imaplib
import email
from email.header import decode_header
import re
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger("bank_analyzer")

# Indian Standard Time (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))


HDFC_SENDERS = ["alerts@hdfcbank.net", "alerts@hdfcbank.bank.in"]
IMAP_SERVER = "imap.gmail.com"
IMAP_PORT = 993


def fetch_hdfc_alerts(
    gmail_user: str,
    gmail_app_password: str,
    days_back: int = 7,
    last_fetch_date: Optional[str] = None,
) -> list[dict]:
    """
    Connect to Gmail via IMAP and fetch HDFC InstaAlert emails.

    Args:
        gmail_user: Gmail address
        gmail_app_password: Gmail App Password (16-char)
        days_back: How many days back to search (default 7)
        last_fetch_date: Optional date string (DD-Mon-YYYY) to fetch since

    Returns:
        List of parsed transaction dicts
    """
    if last_fetch_date:
        since_date = last_fetch_date
    else:
        since = datetime.now(IST) - timedelta(days=days_back)
        since_date = since.strftime("%d-%b-%Y")

    # Connect to Gmail IMAP
    gmail_app_password = gmail_app_password.strip().replace(" ", "")
    mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
    try:
        mail.login(gmail_user, gmail_app_password)
    except imaplib.IMAP4.error as e:
        raise ValueError(
            f"Gmail login failed. Make sure you're using an App Password, not your regular password. Error: {e}"
        )

    mail.select("inbox")

    # Search for HDFC alert emails from all known sender addresses
    id_set = set()
    for sender in HDFC_SENDERS:
        search_criteria = f'(FROM "{sender}" SINCE {since_date})'
        status, message_ids = mail.search(None, search_criteria)
        if status == "OK" and message_ids[0]:
            id_set.update(message_ids[0].split())

    ids = sorted(id_set, key=lambda x: int(x))

    if not ids:
        mail.logout()
        return []

    transactions = []

    for msg_id in ids:
        status, msg_data = mail.fetch(msg_id, "(RFC822)")
        if status != "OK":
            continue

        msg = email.message_from_bytes(msg_data[0][1])

        # Get email date
        email_date = msg.get("Date", "")
        try:
            parsed_date = email.utils.parsedate_to_datetime(email_date)
            txn_date = parsed_date.strftime("%d/%m/%y")
        except Exception:
            txn_date = datetime.now(IST).strftime("%d/%m/%y")

        # Get subject
        subject = _decode_subject(msg.get("Subject", ""))

        # Get body text
        body = _get_email_body(msg)
        if not body:
            continue

        # Parse transaction from body
        txn = _parse_instaalert(body, txn_date, subject)
        if txn:
            transactions.append(txn)

    mail.logout()
    return transactions


def _decode_subject(subject: str) -> str:
    """Decode email subject which may be encoded."""
    decoded_parts = decode_header(subject)
    parts = []
    for part, encoding in decoded_parts:
        if isinstance(part, bytes):
            parts.append(part.decode(encoding or "utf-8", errors="replace"))
        else:
            parts.append(part)
    return " ".join(parts)


def _get_email_body(msg) -> str:
    """Extract plain text or HTML body from email message."""
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            if content_type == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    body = payload.decode("utf-8", errors="replace")
                    break
            elif content_type == "text/html" and not body:
                payload = part.get_payload(decode=True)
                if payload:
                    html = payload.decode("utf-8", errors="replace")
                    # Strip HTML tags for parsing
                    body = re.sub(r"<[^>]+>", " ", html)
                    body = re.sub(r"\s+", " ", body)
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            body = payload.decode("utf-8", errors="replace")
    return body


def _parse_instaalert(body: str, txn_date: str, subject: str) -> Optional[dict]:
    """
    Parse HDFC InstaAlert email body into a transaction dict.

    Common HDFC InstaAlert formats:

    DEBIT:
      "Rs.XXX has been debited from account **3972 on DD-MM-YY to VPA xxx@xxx
       (UPI Ref No XXXX). Avl bal: Rs.XXXXX.XX"

      "Money Sent! Rs.XXX.XX debited from A/c **3972 on DD-Mon-YY.
       Info: UPI/MERCHANT NAME. Avl Bal:Rs.XXXXX.XX"

    CREDIT:
      "Rs.XXX has been credited to your account **3972 on DD-MM-YY by VPA xxx
       (UPI Ref No XXXX). Avl bal: Rs.XXXXX.XX"

      "Money Received! Rs.XXX credited to A/c **3972 on DD-Mon-YY.
       Info: UPI/PERSON. Avl Bal:Rs.XXXXX.XX"
    """
    text = body.replace("\n", " ").replace("\r", " ")
    text = re.sub(r"\s+", " ", text)

    is_debit = False
    is_credit = False

    subject_lower = subject.lower()
    if "debit" in subject_lower or "debited" in subject_lower or "sent" in subject_lower:
        is_debit = True
    elif "credit" in subject_lower or "credited" in subject_lower or "received" in subject_lower:
        is_credit = True

    # Also check body
    text_lower = text.lower()
    if not is_debit and not is_credit:
        if "debited" in text_lower or "money sent" in text_lower:
            is_debit = True
        elif "credited" in text_lower or "money received" in text_lower:
            is_credit = True

    if not is_debit and not is_credit:
        return None

    # Extract amount: Rs.XXX.XX or Rs. XXX.XX or INR XXX.XX
    amount_match = re.search(
        r"(?:Rs\.?|INR)\s*([\d,]+\.?\d*)", text, re.IGNORECASE
    )
    if not amount_match:
        return None
    amount = float(amount_match.group(1).replace(",", ""))

    # Extract balance: "Avl bal: Rs.XXXXX.XX" or "Avl Bal:Rs.XXXXX.XX"
    balance = 0.0
    bal_match = re.search(
        r"(?:Avl\.?\s*bal\.?|Available\s*Balance)[:\s]*(?:Rs\.?|INR)\s*([\d,]+\.?\d*)",
        text,
        re.IGNORECASE,
    )
    if bal_match:
        balance = float(bal_match.group(1).replace(",", ""))

    # Extract date from body if present: DD-MM-YY or DD-Mon-YY or DD/MM/YY
    date_match = re.search(
        r"on\s+(\d{2}[-/]\d{2}[-/]\d{2,4})", text, re.IGNORECASE
    )
    if date_match:
        raw_date = date_match.group(1)
        # Normalize to DD/MM/YY
        txn_date = raw_date.replace("-", "/")
        # Handle DD/Mon/YY format
        if re.match(r"\d{2}/[A-Za-z]{3}/\d{2,4}", txn_date):
            try:
                for fmt in ("%d/%b/%y", "%d/%b/%Y"):
                    try:
                        dt = datetime.strptime(txn_date, fmt)
                        txn_date = dt.strftime("%d/%m/%y")
                        break
                    except ValueError:
                        continue
            except Exception:
                pass
    else:
        # Try DD-Mon-YY format
        date_match2 = re.search(
            r"on\s+(\d{2}-[A-Za-z]{3}-\d{2,4})", text, re.IGNORECASE
        )
        if date_match2:
            try:
                raw = date_match2.group(1)
                for fmt in ("%d-%b-%y", "%d-%b-%Y"):
                    try:
                        dt = datetime.strptime(raw, fmt)
                        txn_date = dt.strftime("%d/%m/%y")
                        break
                    except ValueError:
                        continue
            except Exception:
                pass

    # Extract description/payee info
    description = "HDFC InstaAlert Transaction"

    # Try: "Info: UPI/MERCHANT NAME" or "Info: NEFT/..."
    info_match = re.search(r"Info[:\s]+(.+?)(?:\.\s*Avl|\.?\s*$)", text, re.IGNORECASE)
    if info_match:
        description = info_match.group(1).strip()
    else:
        # Try: "to VPA xxx@xxx" for debits or "by VPA xxx@xxx" for credits
        vpa_match = re.search(r"(?:to|by)\s+VPA\s+(\S+)", text, re.IGNORECASE)
        if vpa_match:
            description = "UPI-" + vpa_match.group(1)
        else:
            # Try: "to XXXX" after amount
            to_match = re.search(
                r"(?:debited|sent).*?(?:to|towards)\s+(.+?)(?:\s*\(|\s*\.?\s*Avl)",
                text,
                re.IGNORECASE,
            )
            if to_match:
                description = to_match.group(1).strip()
            else:
                from_match = re.search(
                    r"(?:credited|received).*?(?:from|by)\s+(.+?)(?:\s*\(|\s*\.?\s*Avl)",
                    text,
                    re.IGNORECASE,
                )
                if from_match:
                    description = from_match.group(1).strip()

    # Clean up description
    description = re.sub(r"\s+", " ", description).strip()
    description = description.rstrip(".")
    if len(description) > 100:
        description = description[:100]

    return {
        "date": txn_date,
        "description": description,
        "debit": amount if is_debit else 0.0,
        "credit": amount if is_credit else 0.0,
        "balance": balance,
        "source": "email_alert",
    }


def fetch_hdfc_balance(gmail_user: str, gmail_app_password: str) -> Optional[dict]:
    """
    Fetch the latest HDFC daily balance notification email from today.

    Returns dict with {balance, date, timestamp} or None.
    """
    gmail_app_password = gmail_app_password.strip().replace(" ", "")
    mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
    try:
        mail.login(gmail_user, gmail_app_password)
    except imaplib.IMAP4.error:
        return None

    mail.select("inbox")

    # Search today and yesterday in IST (Render runs in UTC, HDFC sends at ~7:30 AM IST)
    since_date = (datetime.now(IST) - timedelta(days=1)).strftime("%d-%b-%Y")
    logger.info("Balance: searching since %s (IST now: %s)", since_date, datetime.now(IST).strftime("%Y-%m-%d %H:%M"))

    # Search for balance notification emails from all HDFC senders
    # Subject: "View: Account update for your HDFC Bank A/c"
    ids = []
    for sender in HDFC_SENDERS:
        for subj_term in ["Account update", "balance", "View", "Available"]:
            criteria = f'(FROM "{sender}" SINCE {since_date} SUBJECT "{subj_term}")'
            status, message_ids = mail.search(None, criteria)
            if status == "OK" and message_ids[0]:
                ids.extend(message_ids[0].split())

    if not ids:
        logger.warning("Balance: no emails found matching criteria (since %s)", since_date)
        mail.logout()
        return None

    # Get the latest balance email
    logger.info("Balance: found %d candidate email(s)", len(ids))
    latest_id = ids[-1]
    status, msg_data = mail.fetch(latest_id, "(RFC822)")
    mail.logout()

    if status != "OK":
        logger.warning("Balance: failed to fetch email id %s", latest_id)
        return None

    msg = email.message_from_bytes(msg_data[0][1])
    body = _get_email_body(msg)
    if not body:
        logger.warning("Balance: email body is empty")
        return None

    # Parse balance from email body
    text = re.sub(r"\s+", " ", body.replace("\n", " ").replace("\r", " "))
    logger.info("Balance email body (first 500 chars): %s", text[:500])

    # Try multiple regex patterns to handle format variations
    bal_match = None
    patterns = [
        # Pattern 1: "Available balance ... is Rs. INR 79,542.80 as on 14-APR-26"
        r"(?:available|avl)[\s.]*balance.*?(?:Rs\.?\s*(?:INR\s*)?|INR\s*)([\d,]+\.?\d*)\s*as\s+(?:of|on)\s+(\d{2}-[A-Za-z]{3}-\d{2,4})",
        # Pattern 2: "Your available balance is Rs. 79542.80"
        r"(?:your\s+)?(?:available|avl)[\s.]*balance\s+(?:is|:)\s*(?:Rs\.?\s*(?:INR\s*)?|INR\s*)?([\d,]+\.?\d*)",
        # Pattern 3: More flexible — just find "balance" followed by amount
        r"balance.*?(?:Rs\.?\s*(?:INR\s*)?|INR\s*)([\d,]+\.?\d*)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            bal_match = match
            logger.info("Balance: matched pattern: %s", pattern[:80])
            break

    if not bal_match:
        logger.warning("Balance: no regex pattern matched. Full text: %s", text[:1000])
        return None

    balance = float(bal_match.group(1).replace(",", ""))

    # Try to extract date (may be in group 2 if pattern 1 matched, else fallback)
    date_str = None
    if bal_match.lastindex and bal_match.lastindex >= 2:
        date_str = bal_match.group(2)

    if not date_str:
        date_match = re.search(r"(\d{2}-[A-Za-z]{3}-\d{2,4})", text)
        if date_match:
            date_str = date_match.group(1)
        else:
            date_str = datetime.now(IST).strftime("%d-%b-%y").upper()

    # Parse email timestamp
    email_date = msg.get("Date", "")
    try:
        timestamp = email.utils.parsedate_to_datetime(email_date).isoformat()
    except Exception:
        timestamp = datetime.now(IST).isoformat()

    logger.info("Balance extracted: ₹%.2f on %s", balance, date_str)

    return {
        "balance": balance,
        "date": date_str,
        "timestamp": timestamp,
    }
