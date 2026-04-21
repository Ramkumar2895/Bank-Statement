"""Transaction categorizer for bank statement entries."""

# All available categories (order matches the UI dropdown)
ALL_CATEGORIES = [
    "Bills & Utilities",
    "Business",
    "CC Bill Payment",
    "Education",
    "Entertainment",
    "Financial & Legal",
    "Food",
    "From Family",
    "From Friends",
    "Groceries",
    "Health",
    "Investment",
    "Lend to Others",
    "Lifestyle",
    "Loan EMI Payment",
    "Others",
    "Salary / Income",
    "Shopping",
    "To Family",
    "To Friends",
    "Transportation",
]

# Categories that count as "lent money" (not a real expense)
LEND_CATEGORIES = {"Lend to Others", "To Family", "To Friends"}

CATEGORY_KEYWORDS = {
    "Bills & Utilities": [
        "electricity", "electric", "water bill", "gas bill",
        "broadband", "internet", "wifi", "mobile", "recharge",
        "airtel", "jio", "vodafone", "bsnl", "dth", "tata sky",
        "bill pay", "billdesk", "utility", "postpaid", "prepaid",
        "hathway", "act fibernet", "sms alert",
        "charge", "fee", "gst", "tax", "service charge", "penalty",
        "late fee", "annual fee", "maintenance charge",
        "debit card", "credit card", "bank charge",
    ],
    "Business": [
        "business", "vendor", "merchant", "invoice", "client",
        "consulting", "freelance", "commission",
    ],
    "CC Bill Payment": [
        "cc bill", "credit card bill", "card payment", "cc payment",
        "amex", "visa bill", "mastercard bill",
    ],
    "Financial & Legal": [
        "stamp duty", "legal", "advocate", "court", "notary",
        "registration", "gst payment", "tds",
    ],
    "Food": [
        "swiggy", "zomato", "restaurant", "food", "cafe", "pizza",
        "burger", "dominos", "mcdonalds", "kfc", "starbucks",
        "dunkin", "subway", "dining", "hotel", "dhaba", "biryani",
        "barbeque", "eat", "kitchen", "bakery", "chai",
        "sweets", "fast food", "cream", "irani", "millan",
        "biriyani", "mess", "tiffin", "canteen", "juice",
    ],
    "Groceries": [
        "grocery", "grofers", "blinkit", "zepto", "instamart",
        "bigbasket", "vegetables", "fruits", "milk", "dairy",
        "supermarket", "provision", "kirana",
    ],
    "To Friends": [
        "friend", "split", "settle", "owe",
    ],
    "Entertainment": [
        "netflix", "hotstar", "prime video", "spotify", "youtube",
        "movie", "cinema", "pvr", "inox", "game", "gaming",
        "playstation", "steam", "subscription", "disney",
        "jiocinema", "zee5", "sonyliv",
    ],
    "Shopping": [
        "amazon", "flipkart", "myntra", "ajio", "meesho", "nykaa",
        "shopping", "mall", "store", "retail", "purchase", "mart",
        "bazaar", "dmart", "reliance", "jiomart",
        "croma", "decathlon", "ikea", "shoppers",
    ],
    "Salary / Income": [
        "salary", "sal ", "neft cr", "imps cr",
        "deposit", "refund", "cashback", "interest", "dividend",
        "bonus", "incentive", "stipend", "pension", "wages",
        "indian clearing", "iccl",
    ],
    "Education": [
        "school", "college", "university", "tuition", "course",
        "education", "exam", "fees", "book", "udemy", "coursera",
        "unacademy", "byju", "coaching", "training",
    ],
    "Health": [
        "hospital", "medical", "pharmacy", "medicine", "doctor",
        "clinic", "health", "lab", "diagnostic", "apollo",
        "medplus", "netmeds", "pharmeasy", "1mg", "dental",
        "eye", "therapy",
    ],

    "Investment": [
        "mutual fund", "sip", "mf ", "equity", "shares",
        "stock", "demat", "zerodha", "groww", "upstox",
        "smallcase", "nps", "ppf", "fixed deposit", "fd ",
        "rd ", "gold", "investment", "trading",
    ],
    "Lifestyle": [
        "salon", "spa", "gym", "fitness", "beauty",
        "parlour", "grooming", "tattoo", "piercing",
    ],
    "Loan EMI Payment": [
        "emi", "loan", "mortgage", "home loan", "car loan",
        "personal loan", "education loan", "repayment", "instalment",
        "installment", "nach", "mandate", "enach",
    ],

    "Transportation": [
        "petrol", "diesel", "fuel", "uber", "ola", "rapido",
        "metro", "railway", "irctc", "train", "bus", "auto",
        "parking", "toll", "fastag", "cab", "taxi", "flight",
        "airline", "indigo", "spicejet", "vistara", "makemytrip",
        "redbus",
    ],
}


# Learned categories from user edits (loaded once per session)
_learned_categories = None


def _get_learned_categories() -> dict[str, str]:
    """Lazy-load learned categories from saved data."""
    global _learned_categories
    if _learned_categories is None:
        try:
            from database import get_learned_categories
            _learned_categories = get_learned_categories()
        except Exception:
            _learned_categories = {}
    return _learned_categories


def reload_learned_categories():
    """Force reload learned categories (call after saving)."""
    global _learned_categories
    _learned_categories = None


def categorize_transaction(description: str) -> str:
    """Categorize a transaction based on learned patterns, then keywords."""
    desc_lower = description.lower()

    # 1. Check learned categories from user edits (most specific)
    learned = _get_learned_categories()
    if learned:
        for fragment, category in learned.items():
            if fragment in desc_lower:
                return category

    # 2. Keyword-based matching
    for category, keywords in CATEGORY_KEYWORDS.items():
        for keyword in keywords:
            if keyword in desc_lower:
                return category

    return "Others"


def categorize_transactions(transactions: list[dict]) -> list[dict]:
    """Add category to each transaction."""
    for txn in transactions:
        txn["category"] = categorize_transaction(txn["description"])
    return transactions


def get_category_summary(transactions: list[dict]) -> dict:
    """Summarize spending by category."""
    summary = {}
    for txn in transactions:
        cat = txn.get("category", "Other")
        if txn["debit"] > 0:
            summary[cat] = summary.get(cat, 0) + txn["debit"]
    # Sort by amount descending
    return dict(sorted(summary.items(), key=lambda x: x[1], reverse=True))


def get_monthly_summary(transactions: list[dict]) -> dict:
    """Summarize income vs expense by month."""
    import re
    months = {}
    for txn in transactions:
        date_str = txn["date"]
        # Extract month-year from various formats
        month_key = "Unknown"
        # DD/MM/YYYY or DD-MM-YYYY
        m = re.match(r'\d{2}[/-](\d{2})[/-](\d{2,4})', date_str)
        if m:
            month_num = m.group(1)
            year = m.group(2)
            if len(year) == 2:
                year = "20" + year
            month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                           "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            try:
                month_key = f"{month_names[int(month_num)-1]} {year}"
            except (ValueError, IndexError):
                pass
        else:
            # DD MMM YYYY
            m2 = re.match(r'\d{2}\s+(\w{3})\s+(\d{2,4})', date_str)
            if m2:
                month_key = f"{m2.group(1)} {m2.group(2)}"

        if month_key not in months:
            months[month_key] = {"income": 0, "expense": 0}
        months[month_key]["income"] += txn.get("credit", 0)
        months[month_key]["expense"] += txn.get("debit", 0)

    return months
