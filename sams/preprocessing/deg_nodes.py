import re
import json
import math
import numpy as np
import pandas as pd
from tqdm import tqdm
from loguru import logger
from datetime import date
from typing import Iterable, Optional, Any, Union, Set
import time
import gc

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

    # Drop irrelevant / unnecessary columns, but keep 'phase' if it exists
    columns_to_drop = [
        "student_name",
        "nationality",
        "contact_no",
        "national_cadet_corps",
        "year"
    ]
    columns_to_drop = [col for col in columns_to_drop if col in df.columns and col != "phase"]
    df = df.drop(columns=columns_to_drop, errors="ignore")

    # Drop fully empty columns, except 'phase'
    cols_to_check = [col for col in df.columns if col != "phase"]
    non_all_na_cols = df[cols_to_check].dropna(axis=1, how='all').columns.tolist()
    if "phase" in df.columns:
        non_all_na_cols.append("phase")
    df = df[non_all_na_cols]

    # Sort and drop rows with missing aadhar_no
    df = df.sort_values(by=["aadhar_no", "academic_year"])
    df = df.dropna(subset=["aadhar_no"])

    return df

def preprocess_deg_options_details(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flattens and cleans DEG application option details.

    Parameters
    ----------
    df : pd.DataFrame
        Raw student data with columns:
        - deg_option_details (JSON string of option dicts)
        - aadhar_no, academic_year, barcode

    Returns
    -------
    pd.DataFrame
        Tidy table where each row is one option, including:
        - Context: aadhar_no, academic_year, barcode
        - Normalized option fields (snake_case)
        - num_applications: number of options submitted in that application
    """
    df = df.dropna(subset=["deg_option_details"])
    years = sorted(df["academic_year"].dropna().unique().tolist())
    logger.info(f"Processing DEG options for {len(years)} academic years: {years}")

    parts = []
    start_all = time.time()

    for yr in years:
        start_year = time.time()
        df_year = df[df["academic_year"] == yr].copy()
        logger.info(f"[{yr}] Starting with {len(df_year):,} rows")

        # parse JSON into lists (vectorized)
        df_year["deg_option_details"] = df_year["deg_option_details"].map(json.loads)

        # one row per option
        df_exploded = df_year.explode("deg_option_details", ignore_index=True)

        # Ensure exploded column contains dicts, even for originally empty lists
        df_exploded["deg_option_details"] = df_exploded["deg_option_details"].apply(
            lambda x: x if isinstance(x, dict) else {}
        )

        # normalize nested dicts into columns
        options = pd.json_normalize(df_exploded["deg_option_details"])

        # add back context
        options["aadhar_no"] = df_exploded["aadhar_no"].values
        options["academic_year"] = df_exploded["academic_year"].values
        options["barcode"] = df_exploded["barcode"].values

        # column names to snake_case
        df_options = options.rename(columns=lambda c: camel_to_snake_case(c))

        # number of options actually submitted in the application
        df_options["num_applications"] = (
            df_options.groupby("barcode")["option_no"].transform("count")
        )

        # column order (important first, rest preserved)
        preferred_order = [
            "barcode", "aadhar_no",
            "academic_year", "year", "phase",
            "reported_institute", "sams_code", "institute_district",
            "institute_block", "type_of_institute",
            "stream", "subject",
            "option_no", "admission_status",
            "num_applications",
        ]
        ordered_columns = [col for col in preferred_order if col in df_options.columns]
        remaining_columns = [col for col in df_options.columns if col not in ordered_columns]
        df_options = df_options[ordered_columns + remaining_columns]

        logger.info(f"[{yr}] Finished: {len(df_options):,} option rows in {time.time() - start_year:.1f}s")

        parts.append(df_options)
        del df_year, df_exploded, options, df_options
        gc.collect()

    df_final = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    logger.info(f"All years done: {len(df_final):,} option rows in {time.time() - start_all:.1f}s")
    return df_final


def preprocess_deg_compartments(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess DEG compartment subjects from 'deg_compartments' JSON column.

    Each row in the output corresponds to a single compartment subject.

    Parameters
    ----------
    df : pd.DataFrame
        Raw student data with a 'deg_compartments' column containing JSON lists,
        plus associated metadata like aadhar_no, barcode, etc.

    Returns
    -------
    pd.DataFrame
        Flattened and normalized compartments per student.
    """
    # Drop rows where the field is missing altogether (not just empty list)
    df = df.dropna(subset=["deg_compartments"])

    # Only keep relevant columns
    context_columns = [
        "barcode", "aadhar_no", "board_exam_name_for_highest_qualification",
        "highest_qualification", "module", "academic_year",
        "examination_board_of_the_highest_qualification", "examination_type",
        "year_of_passing", "total_marks", "secured_marks", 
        "percentage", "compartmental_status", "deg_compartments"
    ]
    df = df[[col for col in context_columns if col in df.columns]]

    # Convert JSON string to Python list of dicts
    df["deg_compartments"] = df["deg_compartments"].map(json.loads)

    # Explode the list into multiple rows
    df_exploded = df.explode("deg_compartments", ignore_index=True)

    # Replace any empty exploded value with an empty dict (preserves rows)
    df_exploded["deg_compartments"] = df_exploded["deg_compartments"].apply(
        lambda x: x if isinstance(x, dict) else {}
    )

    # Flatten the dict into separate columns
    compartments = pd.json_normalize(df_exploded["deg_compartments"])
    compartments = compartments.rename(columns=lambda c: camel_to_snake_case(c))

    # Add back context fields
    for col in context_columns:
        if col != "deg_compartments" and col in df_exploded.columns:
            compartments[col] = df_exploded[col].values

    # Order columns: context first, then compartment info
    context_first = [col for col in context_columns if col != "deg_compartments"]
    remaining = [col for col in compartments.columns if col not in context_first]
    compartments = compartments[context_first + remaining]

    logger.info(f"Preprocessed compartments → {len(compartments):,} rows from {len(df):,} students")
    return compartments
