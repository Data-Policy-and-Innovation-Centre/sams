import re
import json
import math
import numpy as np
import pandas as pd
from tqdm import tqdm
from loguru import logger
from datetime import date
from typing import Iterable, Optional, Any, Union, Set
from sams.utils import dict_camel_to_snake_case, camel_to_snake_case, flatten


def _make_null(val: Any, null_tokens: Optional[Iterable[str]] = None) -> Optional[Any]:
    """
    Converts placeholder-like values to None.

    Args:
        val (Any): Value to check.
        null_tokens (Iterable[str]): Custom set of strings to treat as null. 
            Defaults to standard tokens.

    Returns:
        None if value is null-like, else original value.
    """
    default_tokens = {"", "na", "null", "none", "nan", "-", "--"}
    tokens = {t.lower() for t in (null_tokens or default_tokens)}

    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None

    s = str(val).strip().lower()
    return None if s in tokens else val


def _make_bool(val):
    """
    Convert 'YES'/'NO' values to boolean.

    Args:
        val (Any): Value to convert.

    Returns:
        bool | None: True if YES, False if NO, None otherwise.
    """
    if val is None:
        return None

    s = str(val).strip().lower()
    if s == "yes":
        return True
    if s == "no":
        return False
    return None


def _make_date(val: Union[str, int, float, None],
               dayfirst: bool = True,
               allow_year_only: bool = True) -> Optional[date]:
    """
    Parse a value into a date object.

    Args:
        val (str | int | float | None): Value to parse.
        dayfirst (bool): Whether to interpret day before month.
        allow_year_only (bool): If True, a standalone year will default to Jan 1.

    Returns:
        date | None: Parsed date or None if invalid.
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None

    # Handle year-only values
    if allow_year_only:
        try:
            y = int(str(val).strip())
            if 1900 <= y <= 2100:
                return date(y, 1, 1)
        except Exception:
            pass

    try:
        ts = pd.to_datetime(val, errors="coerce", dayfirst=dayfirst, infer_datetime_format=True)
        if pd.isna(ts):
            return None
        return ts.date()
    except Exception:
        return None


def _correct_address(address: Optional[str]) -> Optional[str]:
    """
    Clean and normalize address strings (spacing, commas, casing).

    Args:
        val (str): Raw string

    Returns:
        str: Corrected address or None if invalid
    """
    if not address:
        return None

    # Convert to string, strip spaces
    addr = str(address).strip()
    if not addr:
        return None

    # Collapse spaces, standardize commas, fix hyphens, collapse repeated commas
    addr = re.sub(r"\s+", " ", addr)
    addr = re.sub(r"\s*,\s*", ", ", addr)
    addr = re.sub(r"(?<=\w)\s*-\s*(?=\w)", " - ", addr)
    addr = re.sub(r"(,\s*){2,}", ", ", addr)

    return addr.title()

def _fix_qual_names(series: pd.Series) -> pd.Series:
    """
    Clean and format highest qualification values.

    Args:
        series (pd.Series): Input series with qualification names

    Returns:
        pd.Series: Cleaned and title-cased qualification names
    """
    def fix(val):
        val = _make_null(val)
        if val is None:
            return None
        val = str(val).strip().upper()
        if val.startswith("+2 "):
            return "+2 " + val[3:].title()
        return val.title()

    return series.map(fix)


def _normalize_text(val: Union[str, float, int, None], camel_to_snake: bool = True) -> Optional[str]:
    """
    Normalize string: strip, lowercase, remove extra spaces.
    Converts CamelCase to snake_case using camel_to_snake_case().

    Args:
        val (str): Raw string
        camel_to_snake (bool): If True, apply camel_to_snake_case conversion.

    Returns:
        str: Normalized string or None if invalid
    """
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    if not isinstance(val, str):
        val = str(val)

    # Strip and collapse spaces
    text = " ".join(val.strip().split())

    # Apply camelCase → snake_case if desired
    if camel_to_snake:
        text = camel_to_snake_case(text)

    # Ensure lowercase and replace spaces/hyphens with underscores
    text = text.lower().replace(" ", "_").replace("-", "_")

    # Remove multiple underscores
    while "__" in text:
        text = text.replace("__", "_")

    return text or None

def _preprocess_students(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize student data for analysis.

    Converts placeholders to None, parses dates, maps YES/NO to booleans, and normalizes addresses.

    Args:
        df (pd.DataFrame): Raw student data.

    Returns:
        pd.DataFrame: Preprocessed student data ready for analysis (modified in place).
    """

    # Null-normalization across all columns
    df[:] = df.applymap(_make_null)

    # Dates
    if "dob" in df.columns:
        df["dob"] = df["dob"].map(lambda v: _make_date(v, dayfirst=True, allow_year_only=False))

    # YES/NO → boolean
    bool_vars = ["es", "national_cadet_corps", "orphan", "ph", "sports"]
    for col in bool_vars:
        if col in df.columns:
            df[col] = df[col].map(_make_bool)

    # Address cleanup
    if "address" in df.columns:
        def _compose_address(row) -> Optional[str]:
            base = _make_null(row.get("address"))
            if base:
                return _correct_address(base)
            parts = [
                _make_null(row.get("block")),
                _make_null(row.get("district")),
                _make_null(row.get("state")),
                _make_null(row.get("pin_code")),
            ]
            parts = [str(p) for p in parts if p is not None]
            return _correct_address(", ".join(parts)) if parts else None

        df["address"] = df.apply(_compose_address, axis=1)

    return df


def preprocess_deg_students_enrollment_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess Higher education student enrollment data.

    Args:
        df (pd.DataFrame): Raw degree student data

    Returns:
        pd.DataFrame: Cleaned and preprocessed degree student data
    """
    # Standard preprocessing
    df = _preprocess_students(df)

    if "highest_qualification" in df.columns:
        df["highest_qualification"] = _fix_qual_names(df["highest_qualification"])

    # Drop irrelevant / unnecessary columns
    df = df.drop(
        columns=[
            "student_name",                
            "nationality",                
            "contact_no",                 
            "national_cadet_corps",               ],
        errors="ignore"
    )

    # Drop fully empty columns, and sort for consistency 
    df = df.dropna(axis=1, how='all')
    df = df.sort_values(by=["aadhar_no", "academic_year"])
    df = df.dropna(subset=["aadhar_no"])
    return df
