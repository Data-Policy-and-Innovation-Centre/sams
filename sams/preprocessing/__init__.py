import ibis
from typing import Optional, Iterable

def normalize_nulls(
    col: ibis.Expr,
    null_tokens: Optional[Iterable[str]] = None
) -> ibis.Expr:
    """Standardize placeholder values into NULL."""
    default_tokens = ["", " ", "na", "null", "none", "nan", "-", "--"]
    tokens = [t.lower() for t in (null_tokens or default_tokens)]
    s = col.cast("string").strip().lower()

    cleaned = (
        ibis.case()
        .when(s.isin(tokens), ibis.null())
        .else_(s)
        .end()
    )
    return cleaned.cast(col.type()) if not col.type().is_string() else cleaned


def make_bool(col: ibis.Expr, true_val: str = "yes", false_val: str = "no") -> ibis.Expr:
    """Convert YES/NO-like string values to Boolean (True/False/NULL)."""
    s = col.cast("string").strip().lower()
    return (
        ibis.case()
        .when(s == true_val.lower(), ibis.literal(True))
        .when(s == false_val.lower(), ibis.literal(False))
        .else_(ibis.null())
        .end()
    ).cast("boolean")

import ibis

def normalize_date(col: ibis.Expr) -> ibis.Expr:
    """
    Normalize date-like strings into canonical DATE (YYYY-MM-DD).

    Supported input formats:
      - YYYY-MM-DD
      - DD-MM-YYYY or D-M-YYYY
      - DD-Mon-YYYY or D-Mon-YYYY (with 3-letter month name, case-insensitive)
    """
    s = col.cast("string").strip().lower()

    # 1. Direct ISO (YYYY-MM-DD)
    iso = s.try_cast("date")

    # 2. Numeric DMY (DD-MM-YYYY or D-M-YYYY)
    parts = s.split("-")
    dmy_str = ibis.case().when(
        parts.length() == 3,
        parts[2] + "-" + parts[1].lpad(2, "0") + "-" + parts[0].lpad(2, "0")
    ).else_(None).end()
    dmy = dmy_str.try_cast("date")

    # 3. DD-Mon-YYYY (e.g. 27-Feb-2001 or 7-Feb-2001)
    month_map = {
        "jan": "01","feb": "02","mar": "03","apr": "04","may": "05","jun": "06",
        "jul": "07","aug": "08","sep": "09","oct": "10","nov": "11","dec": "12",
    }
    mon_parts = s.split("-")
    mon_num = ibis.case()
    for abbr, num in month_map.items():
        mon_num = mon_num.when(mon_parts[1] == abbr, num)
    mon_num = mon_num.else_(None).end()

    mon_str = ibis.case().when(
        (mon_parts.length() == 3) & mon_num.notnull(),
        mon_parts[2] + "-" + mon_num + "-" + mon_parts[0].lpad(2, "0")
    ).else_(None).end()
    mon = mon_str.try_cast("date")

    # Combine all parsing attempts
    return iso.fillna(dmy).fillna(mon)
