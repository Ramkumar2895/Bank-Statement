import fitz  # PyMuPDF
import pdfplumber
import re
import io
from typing import Optional


def check_pdf_encrypted(file_bytes: bytes) -> bool:
    """Check if a PDF file is password-protected."""
    try:
        doc = fitz.open(stream=file_bytes, filetype="pdf")
        is_encrypted = doc.is_encrypted
        doc.close()
        return is_encrypted
    except Exception:
        return True


def extract_text_from_pdf(file_bytes: bytes, password: Optional[str] = None) -> str:
    """Extract text from PDF, handling password-protected files."""
    try:
        doc = fitz.open(stream=file_bytes, filetype="pdf")

        if doc.is_encrypted:
            if not password:
                doc.close()
                raise ValueError("PDF is password-protected. Please provide the password.")
            authenticated = doc.authenticate(password)
            if not authenticated:
                doc.close()
                raise ValueError("Incorrect password. Please try again.")

        full_text = ""
        for page in doc:
            full_text += page.get_text() + "\n"
        doc.close()

        if full_text.strip():
            return full_text

    except ValueError:
        raise
    except Exception:
        pass

    # Fallback to pdfplumber
    try:
        pdf_io = io.BytesIO(file_bytes)
        with pdfplumber.open(pdf_io, password=password or "") as pdf:
            full_text = ""
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    full_text += text + "\n"
        return full_text
    except Exception as e:
        raise ValueError(f"Failed to read PDF: {str(e)}")


def parse_transactions(text: str, file_bytes: bytes = None, password: Optional[str] = None) -> list[dict]:
    """Parse bank statement text into structured transactions."""
    if _is_hdfc_format(text):
        transactions = _parse_hdfc_text(text)
        if transactions:
            return transactions
        # Fallback: try pdfplumber text (different formatting)
        if file_bytes:
            transactions = _parse_hdfc_pdfplumber(file_bytes, password)
            if transactions:
                return transactions

    if _is_sbi_format(text):
        if file_bytes:
            transactions = _parse_sbi_pdfplumber(file_bytes, password)
            if transactions:
                return transactions
        # Fallback to generic
        return _parse_generic_transactions(text)

    return _parse_generic_transactions(text)


def _is_hdfc_format(text: str) -> bool:
    """Detect HDFC bank statement format."""
    upper = text.upper()
    return "HDFC BANK" in upper or "HDFCBANK" in upper or "HDFC0" in upper


def _is_sbi_format(text: str) -> bool:
    """Detect SBI bank statement format.

    Uses strong institutional markers rather than IFSC codes which can
    appear in UPI transaction descriptions for any bank.
    """
    upper = text.upper()
    if "STATE BANK OF INDIA" in upper:
        return True
    # Check for SBI identifiers only in the first 2000 chars of each page
    # to avoid matching SBIN0 IFSC codes in UPI transaction descriptions
    for page_marker in upper.split("PAGE NO"):
        header = page_marker[:2000]
        if "SBIN0" in header or "SBI." in header:
            return True
    return False


def _clean_narration(val) -> str:
    """Clean a narration/description cell value."""
    if val is None:
        return ""
    desc = str(val).replace('\n', ' ')
    desc = re.sub(r'\b\d{10,}\b', '', desc)
    desc = re.sub(r'0{4,}\d+', '', desc)
    desc = re.sub(r'\s+', ' ', desc).strip()
    desc = desc.rstrip(' -/')
    return desc


def _parse_hdfc_text(text: str) -> list[dict]:
    """Parse HDFC statement from PyMuPDF text using amount-pair detection.

    PyMuPDF extracts table cells as individual lines. Amounts appear as
    standalone lines in consecutive pairs: [txn_amount, closing_balance].
    We find these pairs and look backward for dates and narration.
    """
    transactions = []
    amount_line_re = re.compile(r'^[\d,]+\.\d{2}$')
    date_start_re = re.compile(r'^(\d{2}/\d{2}/\d{2,4})\b')
    pure_date_re = re.compile(r'^\d{2}/\d{2}/\d{2,4}$')
    ref_re = re.compile(r'^\d{10,}$')

    raw_lines = text.split('\n')
    lines = [l.strip() for l in raw_lines]

    # Find end of transactions (statement summary)
    end_idx = len(lines)
    for i, line in enumerate(lines):
        if 'STATEMENT SUMMARY' in line.upper():
            end_idx = i
            break

    txn_lines = lines[:end_idx]

    # Extract opening balance from summary section
    opening_balance = None
    summary_text = text[text.upper().find('OPENING BALANCE'):] if 'OPENING BALANCE' in text.upper() else ''
    if summary_text:
        amounts = re.findall(r'[\d,]+\.\d{2}', summary_text[:300])
        if amounts:
            opening_balance = float(amounts[0].replace(',', ''))

    # Find all standalone amount lines
    amount_indices = []
    for i, line in enumerate(txn_lines):
        if amount_line_re.match(line):
            amount_indices.append(i)

    # Group into consecutive pairs: (txn_amount_idx, balance_idx)
    pairs = []
    i = 0
    while i < len(amount_indices) - 1:
        if amount_indices[i + 1] - amount_indices[i] == 1:
            pairs.append((amount_indices[i], amount_indices[i + 1]))
            i += 2
        else:
            i += 1

    if not pairs:
        return []

    prev_balance = opening_balance
    prev_end = -1  # end index of previous transaction's balance line

    for amt_idx, bal_idx in pairs:
        txn_amount = float(txn_lines[amt_idx].replace(',', ''))
        closing_balance = float(txn_lines[bal_idx].replace(',', ''))

        # Find the transaction date: first date line after prev_end
        txn_date = None
        txn_date_idx = None
        for j in range(prev_end + 1, amt_idx):
            dm = date_start_re.match(txn_lines[j])
            if dm:
                txn_date = dm.group(1)
                txn_date_idx = j
                break

        if txn_date is None:
            prev_end = bal_idx
            continue

        # Collect narration: lines between txn_date and the amount,
        # excluding pure dates (value date) and ref numbers
        narration_parts = []
        # Check if there's text on the same line as the date
        rest_of_date_line = txn_lines[txn_date_idx][date_start_re.match(txn_lines[txn_date_idx]).end():].strip()
        if rest_of_date_line:
            narration_parts.append(rest_of_date_line)

        for j in range(txn_date_idx + 1, amt_idx):
            line = txn_lines[j]
            if not line:
                continue
            if pure_date_re.match(line):
                continue
            if ref_re.match(line):
                continue
            narration_parts.append(line)

        description = ' '.join(narration_parts)
        description = _clean_narration(description)
        if not description:
            description = "Unknown Transaction"

        # Determine debit/credit using balance tracking
        debit = 0.0
        credit = 0.0
        if prev_balance is not None:
            if abs(round(prev_balance - txn_amount, 2) - closing_balance) < 0.02:
                debit = txn_amount
            elif abs(round(prev_balance + txn_amount, 2) - closing_balance) < 0.02:
                credit = txn_amount
            else:
                debit = txn_amount
        else:
            debit = txn_amount

        transactions.append({
            'date': txn_date,
            'description': description,
            'debit': debit,
            'credit': credit,
            'balance': closing_balance,
        })

        prev_balance = closing_balance
        prev_end = bal_idx

    return transactions


def _parse_hdfc_pdfplumber(file_bytes: bytes, password: Optional[str] = None) -> list[dict]:
    """Fallback: parse HDFC statement using pdfplumber text extraction."""
    try:
        pdf_io = io.BytesIO(file_bytes)
        with pdfplumber.open(pdf_io, password=password or "") as pdf:
            full_text = ""
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    full_text += text + "\n"
    except Exception:
        return []

    if not full_text.strip():
        return []

    transactions = []
    date_line_re = re.compile(r'^(\d{2}/\d{2}/\d{2,4})\s+(.*)')
    amount_re = re.compile(r'(?<!\d)[\d,]+\.\d{2}(?!\d)')

    # Extract opening balance
    opening_balance = None
    ob_match = re.search(r'opening\s*balance', full_text, re.IGNORECASE)
    if ob_match:
        after_ob = full_text[ob_match.end():ob_match.end() + 300]
        ob_amounts = amount_re.findall(after_ob)
        if ob_amounts:
            opening_balance = float(ob_amounts[0].replace(',', ''))

    lines = full_text.split('\n')

    # Find transaction lines (start with date and have amounts)
    blocks = []
    current_block = None
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if re.search(r'statement\s*summary', stripped, re.IGNORECASE):
            break

        match = date_line_re.match(stripped)
        if match and amount_re.search(match.group(2)):
            if current_block:
                blocks.append(current_block)
            current_block = {
                'date': match.group(1),
                'first_line': match.group(2),
                'continuation': [],
            }
        elif current_block is not None:
            current_block['continuation'].append(stripped)

    if current_block:
        blocks.append(current_block)

    prev_balance = opening_balance
    for block in blocks:
        amounts = amount_re.findall(block['first_line'])
        parsed_amounts = [float(a.replace(',', '')) for a in amounts]

        if len(parsed_amounts) < 2:
            continue

        closing_balance = parsed_amounts[-1]
        txn_amount = parsed_amounts[-2]

        debit = 0.0
        credit = 0.0
        if prev_balance is not None:
            if abs(round(prev_balance - txn_amount, 2) - closing_balance) < 0.02:
                debit = txn_amount
            elif abs(round(prev_balance + txn_amount, 2) - closing_balance) < 0.02:
                credit = txn_amount
            else:
                debit = txn_amount
        else:
            debit = txn_amount

        # Description: text before first long number or value date
        desc_end = re.search(r'\b\d{10,}\b|\b\d{2}/\d{2}/\d{2}\b', block['first_line'])
        if desc_end:
            description = block['first_line'][:desc_end.start()].strip()
        else:
            first_amt = amount_re.search(block['first_line'])
            description = block['first_line'][:first_amt.start()].strip() if first_amt else block['first_line']

        description = _clean_narration(description)
        if not description:
            description = "Unknown Transaction"

        transactions.append({
            'date': block['date'],
            'description': description,
            'debit': debit,
            'credit': credit,
            'balance': closing_balance,
        })
        prev_balance = closing_balance

    return transactions


def _parse_sbi_pdfplumber(file_bytes: bytes, password: Optional[str] = None) -> list[dict]:
    """Parse SBI statement using pdfplumber table extraction.

    SBI table columns: [txn_date, value_date, description, -, debit, credit, balance]
    """
    try:
        pdf_io = io.BytesIO(file_bytes)
        with pdfplumber.open(pdf_io, password=password or "") as pdf:
            all_rows = []
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        # Skip header rows and empty rows
                        if not row or not row[0]:
                            continue
                        # Skip summary table rows
                        first_cell = str(row[0]).strip()
                        if 'statement summary' in first_cell.lower():
                            break
                        if 'brought forward' in first_cell.lower():
                            continue
                        # Must start with a date DD/MM/YYYY
                        if not re.match(r'\d{2}/\d{2}/\d{4}', first_cell):
                            continue
                        all_rows.append(row)
    except Exception:
        return []

    if not all_rows:
        return []

    transactions = []
    date_re = re.compile(r'\d{2}/\d{2}/\d{4}')

    for row in all_rows:
        if len(row) < 7:
            continue

        txn_date = str(row[0]).strip()
        if not date_re.match(txn_date):
            continue

        # Column 2: description (may contain \n)
        raw_desc = str(row[2]) if row[2] else ""
        # Clean description: take the meaningful parts, skip ref numbers and branch
        desc_lines = [l.strip() for l in raw_desc.split('\n') if l.strip()]
        # Filter out reference number lines and branch lines
        meaningful = []
        for line in desc_lines:
            # Skip lines that are purely reference numbers
            if re.match(r'^\d{10,}\s', line):
                continue
            # Skip branch location lines
            if re.match(r'^[A-Z]+,\s*[A-Z]+', line) and len(line) < 30:
                continue
            meaningful.append(line)
        description = ' '.join(meaningful)
        description = _clean_narration(description)
        if not description:
            description = "Unknown Transaction"

        # Column 4: debit, Column 5: credit, Column 6: balance
        debit_str = str(row[4]).strip() if row[4] else "-"
        credit_str = str(row[5]).strip() if row[5] else "-"
        balance_str = str(row[6]).strip() if row[6] else "0"

        debit = 0.0
        credit = 0.0
        balance = 0.0

        if debit_str and debit_str != '-':
            try:
                debit = float(debit_str.replace(',', ''))
            except ValueError:
                pass
        if credit_str and credit_str != '-':
            try:
                credit = float(credit_str.replace(',', ''))
            except ValueError:
                pass
        if balance_str and balance_str != '-':
            try:
                balance = float(balance_str.replace(',', '').replace('CR', '').replace('DR', '').strip())
            except ValueError:
                pass

        transactions.append({
            'date': txn_date,
            'description': description,
            'debit': debit,
            'credit': credit,
            'balance': balance,
        })

    return transactions


def _parse_generic_transactions(text: str) -> list[dict]:
    """Generic parser for non-HDFC bank statement formats."""
    transactions = []

    # Common date formats in Indian bank statements
    date_patterns = [
        r'(\d{2}[/-]\d{2}[/-]\d{4})',
        r'(\d{2}[/-]\d{2}[/-]\d{2})',
        r'(\d{2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4})',
        r'(\d{2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{2})',
    ]

    amount_pattern = r'[\d,]+\.\d{2}'
    lines = text.split('\n')

    for line in lines:
        line = line.strip()
        if not line:
            continue

        date_found = None
        for pattern in date_patterns:
            match = re.search(pattern, line, re.IGNORECASE)
            if match:
                date_found = match.group(1)
                break

        if not date_found:
            continue

        amounts = re.findall(amount_pattern, line)
        if not amounts:
            continue

        desc_start = line.find(date_found) + len(date_found)
        first_amount_match = re.search(amount_pattern, line[desc_start:])
        if first_amount_match:
            description = line[desc_start:desc_start + first_amount_match.start()].strip()
        else:
            description = line[desc_start:].strip()

        description = re.sub(r'\s+', ' ', description).strip()
        description = description.strip('/ -|')

        if not description:
            description = "Unknown Transaction"

        parsed_amounts = [float(a.replace(',', '')) for a in amounts]

        if len(parsed_amounts) >= 2:
            debit = 0.0
            credit = 0.0

            line_upper = line.upper()
            if 'DR' in line_upper or 'DEBIT' in line_upper:
                debit = parsed_amounts[0]
            elif 'CR' in line_upper or 'CREDIT' in line_upper:
                credit = parsed_amounts[0]
            else:
                if len(parsed_amounts) >= 3:
                    debit = parsed_amounts[0]
                    credit = parsed_amounts[1]
                else:
                    debit = parsed_amounts[0]

            transactions.append({
                "date": date_found,
                "description": description,
                "debit": debit,
                "credit": credit,
                "balance": parsed_amounts[-1] if len(parsed_amounts) >= 2 else 0.0,
            })
        elif len(parsed_amounts) == 1:
            line_upper = line.upper()
            is_credit = any(kw in line_upper for kw in ['CR', 'CREDIT', 'SALARY', 'NEFT CR', 'IMPS CR', 'RECEIVED', 'DEPOSIT'])
            transactions.append({
                "date": date_found,
                "description": description,
                "debit": 0.0 if is_credit else parsed_amounts[0],
                "credit": parsed_amounts[0] if is_credit else 0.0,
                "balance": 0.0,
            })

    return transactions
