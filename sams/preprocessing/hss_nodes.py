import pandas as pd
import numpy as np
import json
from sams.utils import dict_camel_to_snake_case, flatten
from pandarallel import pandarallel
from loguru import logger
from tqdm import tqdm

def _make_null(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace empty strings, spaces and "NA" with NaN in a DataFrame

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to be cleaned

    Returns
    -------
    pd.DataFrame
        DataFrame with NaN instead of empty strings, spaces and "NA"
    """
    return df.replace({"": np.nan, " ": np.nan, "NA": np.nan})

def _make_bool(x: pd.Series, true_val: str = "Yes", false_val: str = "No") -> pd.Series:
    """
    Convert specific string values in a pandas Series to boolean values

    Parameters
    ----------
    x : pd.Series
        Series containing string values to be converted
    true_val : str, optional
        String value to be converted to True, by default "Yes"
    false_val : str, optional
        String value to be converted to False, by default "No"

    Returns
    -------
    pd.Series
        Series with values converted to boolean
    """
    return x.map({true_val: True, false_val: False})

def _make_date(x: pd.Series) -> pd.Series:
    """
    Convert string values in a Series to datetime format (safe conversion).

    Parameters
    ----------
    x : pd.Series
        Column with date values like 'year_of_passing'

    Returns
    -------
    pd.Series
        Datetime64 Series (NaT if invalid)
    """
    return pd.to_datetime(x, errors='coerce')


def _clean_year_of_passing(x: pd.Series) -> pd.Series:
    """
    Clean year_of_passing column by coercing to numeric and removing invalid years.
    """
    x = pd.to_numeric(x, errors="coerce")
    x = x.where((x >= 1970) & (x <= 2025), np.nan)
    return x

def _clean_percentage(x: pd.Series) -> pd.Series:
    """
    Clean percentage values by coercing to float and removing out-of-bound values.
    """
    x = pd.to_numeric(x, errors="coerce")
    return x.where((x >= 0) & (x <= 100), np.nan)


def _coerce_marks(x: pd.Series) -> pd.Series:
    return pd.to_numeric(x, errors="coerce")


def _correct_addresses(address: str, block: str, district: str, state: str, pincode: str) -> str:
    """
    Correct addresses by adding block, district and state if not present

    Parameters
    ----------
    address : str
        Original address
    block : str
        Block
    district : str
        District
    state : str
        State

    Returns
    -------
    str
        Corrected address
    """
    try:
        return f"{address.split(', ')[0]}, {block}, {district}, {state} {pincode}"
    except AttributeError:
        return f"{block}, {district}, {state} {pincode or ''}".strip()


def _preprocess_hss_students(df: pd.DataFrame, geocode=True) -> pd.DataFrame:
    """
    Preprocess HSS student data.

    Parameters
    ----------
    df : pd.DataFrame
        Raw HSS student data
    geocode : bool, optional
        Whether to geocode address, by default True

    Returns
    -------
    pd.DataFrame
        Preprocessed student data
    """

    # Null cleanup
    df = _make_null(df)
    
    # Date cleanup
    if "dob" in df.columns:
        df["dob"] = _make_date(df["dob"])
    
    # Boolean fields cleanup
    bool_cols = ["ph", "es", "sports", "national_cadet_corps", "orphan", "compartmental_status"]
    for col in bool_cols:
        if col in df.columns:
            df[col] = _make_bool(df[col])
    
    # Fix percentage values
    if "percentage" in df.columns:
        df["percentage"] = _clean_percentage(df["percentage"])

    # Fix year_of_passing
    if "year_of_passing" in df.columns:
        df["year_of_passing"] = _clean_year_of_passing(df["year_of_passing"])
    
    # Coerce marks to numeric
    for col in ["secured_marks", "total_marks"]:
        if col in df.columns:
            df[col] = _coerce_marks(df[col])

    #  Add corrected address
    if all(col in df.columns for col in ["address", "block", "district", "state", "pin_code"]):
        df["full_address"] = df.apply(
            lambda row: _correct_addresses(
                row["address"],
                row["block"],
                row["district"],
                row["state"],
                row["pin_code"]
            ), axis=1
        )

    return df


def _preprocess_income_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    doc-string
    """
    if "annual_income" in df.columns:
        df["annual_income"] = df["annual_income"].astype(str).str.strip()
    return df

def extract_hss_options(df: pd.DataFrame, option_col: str = "hss_option_details", id_col: str = "barcode", year_col: str = "academic_year") -> pd.DataFrame:
    """
    Flatten the 'hss_option_details' JSON field into long format.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing a JSON string column with option data.
    option_col : str
        The name of the column containing JSON string (list of option dicts).
    id_col : str
        The column used to carry over the student ID ('barcode').
    year_col : str
        The column for academic year to carry through in the output.

    Returns
    -------
    pd.DataFrame
        A long-format DataFrame with individual option records, each including
        student ID and academic year. If the list is empty or invalid, returns a row
        with None values for options and includes the ID and year.
    """
    records = []

    for _, row in df.iterrows():
        barcode = row.get(id_col)
        academic_year = row.get(year_col)
        raw = row.get(option_col)

        if pd.isna(raw):
            records.append({id_col: barcode, year_col: academic_year})
            continue

        try:
            options = json.loads(raw) if isinstance(raw, str) else raw

            if isinstance(options, list) and options:
                for option in options:
                    record = option.copy()
                    record[id_col] = barcode
                    record[year_col] = academic_year
                    records.append(record)
            else:
                records.append({id_col: barcode, year_col: academic_year})

        except Exception:
            records.append({id_col: barcode, year_col: academic_year})

    return pd.DataFrame(records)



def extract_hss_compartments(df: pd.DataFrame, compartment_col: str = "hss_compartments", id_col: str = "barcode", year_col: str = "academic_year") -> pd.DataFrame:
    """
    Flatten the 'hss_compartments' JSON field into long format.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing a JSON string column with compartment data.
    compartment_col : str
        The name of the column containing JSON string (list of subject dicts).
    id_col : str
        The column used to carry over the student ID ('barcode').

    Returns
    -------
    pd.DataFrame
        A long-format DataFrame with COMPSubject, COMPFailMark, COMPPassMark, and barcode.
        If the JSON list is empty, returns row with only barcode.
    """
    records = []

    for _, row in df.iterrows():
        barcode = row.get(id_col)
        academic_year = row.get(year_col)
        raw = row.get(compartment_col)

        if pd.isna(raw):
            records.append({"COMPSubject": None, "COMPFailMark": None, "COMPPassMark": None, id_col: barcode, year_col: academic_year})
            continue

        try:
            compartments = json.loads(raw)

            if isinstance(compartments, list) and compartments:
                for subject in compartments:
                    record = subject.copy()
                    record[id_col] = barcode
                    record[year_col] = academic_year
                    records.append(record)
            else:
                records.append({"COMPSubject": None, "COMPFailMark": None, "COMPPassMark": None, id_col: barcode, year_col: academic_year})

        except Exception:
            records.append({"COMPSubject": None, "COMPFailMark": None, "COMPPassMark": None, id_col: barcode, year_col: academic_year})

    return pd.DataFrame(records)


def preprocess_students_compartment_marks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten and preprocess HSS compartment subject marks.

    Returns a long-format DataFrame with one row per failed subject (if any).
    """
    records = []

    for _, row in df.iterrows():
        barcode = row.get("barcode")
        year = row.get("academic_year")
        status = row.get("compartmental_status")
        compartments_raw = row.get("hss_compartments")

        if not compartments_raw or str(compartments_raw).strip() in ["", "[]", "null", "None"]:
            continue

        try:
            compartments = json.loads(compartments_raw) if isinstance(compartments_raw, str) else compartments_raw
            if isinstance(compartments, list) and compartments:
                for subject in compartments:
                    records.append({ "barcode": barcode, "academic_year": year, "subject": subject.get("COMPSubject"), "failed_mark": subject.get("COMPFailMark"), "pass_mark": subject.get("COMPPassMark"), "compartmental_status": status })
        except Exception as e:
            print(f"Skipping malformed row: {e}")
            continue

    return pd.DataFrame(records)


def preprocess_hss_students_enrollment_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess HSS student enrollment data for downstream use.
    
    Parameters
    ----------
    df : pd.DataFrame
        Raw HSS student data.
    
    Returns
    -------
    pd.DataFrame
        Cleaned and preprocessed HSS enrollment data.
    """

    # Core cleaning from internal helper
    df = _preprocess_hss_students(df, geocode=False)

    # Drop other unnecessary columns
    drop_cols = [
        "student_name", "nationality", "address", "pin_code",
        "board_exam_namefor_highest_qualification", "examination_type",
        "examination_boardofthe_highest_qualification", "national_cadet_corps",
        "orphan", "sports", "year_of_passing"
    ]
    cols_to_drop = [col for col in drop_cols if col in df.columns]
    df = df.drop(columns=cols_to_drop)

    # Clean income if present
    if "annual_income" in df.columns:
        df = _preprocess_income_data(df)

    # Sort by barcode and academic_year if they exist
    sort_cols = [col for col in ["barcode", "academic_year"] if col in df.columns]
    if sort_cols:
        df = df.sort_values(by=sort_cols)

    return df
 
def get_priority_admission_status(df: pd.DataFrame, option_col: str = "hss_option_details", id_col: str = "barcode") -> pd.DataFrame:
    """
    Extract the most relevant HSS option per student from hss_option_details.

    Logic:
    - Prioritize options in the order: ADMITTED > SELECTED BUT NOT ADMITTED > TC TAKEN > NOT SELECTED
    - Return one row per student with fields from the selected option.

    Parameters
    ----------
    hss_option_details : pd.Series
        A series of JSON strings or lists (one per student).

    Returns
    -------
    pd.DataFrame
        One row per student, with details of the admitted/selected HSS option.
    """
    extracted = []

    preferred_statuses = [
        "ADMITTED",
        "SELECTED BUT NOT ADMITTED",
        "TC TAKEN",
        "NOT SELECTED"
    ]

    for i, row in df.iterrows():
        student_id = row.get(id_col)
        options = row.get(option_col)

        try:
            parsed = json.loads(options) if isinstance(options, str) else options
            selected = None

            for status in preferred_statuses:
                selected = next((opt for opt in parsed if opt.get("AdmissionStatus") == status), None)
                if selected:
                    break

            if selected:
                selected = selected.copy()
                selected[id_col] = student_id
                extracted.append(selected)
            else:
                extracted.append({id_col: student_id})

        except (TypeError, json.JSONDecodeError):
            extracted.append({id_col: student_id})

    return pd.DataFrame(extracted)



def filter_admitted_on_first_choice(hss_admitted_option: pd.DataFrame) -> pd.DataFrame:
    """
    Filter students who were ADMITTED in their Option 1 choice.

    Parameters
    ----------
    hss_admitted_option : pd.DataFrame
        Flattened admitted options per student from hss_option_details.

    Returns
    -------
    pd.DataFrame
        Subset with only students admitted on Option 1.
    """
    filtered = hss_admitted_option[
        (hss_admitted_option["OptionNo"].astype(str).str.strip() == "1") &
        (hss_admitted_option["AdmissionStatus"].fillna("").str.strip().str.upper() == "ADMITTED")
    ]
    return filtered

def analyze_stream_trends(df: pd.DataFrame, option_col: str = "hss_option_details") -> pd.DataFrame:
    """
    Analyze HSS stream selection trends over years from hss_option_details.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with hss_option_details column.
    option_col : str
        Column name containing the JSON options (default: 'hss_option_details').

    Returns
    -------
    pd.DataFrame
        Aggregated counts of admitted students per Stream per Year.
    """
    records = []

    for _, row in df.iterrows():
        options_raw = row.get(option_col)
        if pd.isna(options_raw):
            continue

        try:
            options = json.loads(options_raw) if isinstance(options_raw, str) else options_raw
            for opt in options:
                records.append({
                    "barcode": row["barcode"],
                    "Year": opt.get("Year"),
                    "Stream": opt.get("Stream"),
                    "Institute": opt.get("ReportedInstitute"),
                    "District": opt.get("InstituteDistrict"),
                })
        except (json.JSONDecodeError, TypeError):
            continue

    df_streams = pd.DataFrame(records)
    stream_summary = df_streams.groupby(["Year", "Stream"]).size().reset_index(name="student_count")
    return stream_summary.sort_values(["Year", "student_count"], ascending=[True, False])


def compute_local_flag(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes whether student is local to the admitted institute.

    Parameters
    ----------
    df : pd.DataFrame
        Enriched student data with admitted institute info

    Returns
    -------
    pd.DataFrame
        Same DataFrame with 'local' boolean column
    """
    if "district" not in df.columns or "InstituteDistrict" not in df.columns:
        raise ValueError("Missing 'district' or 'InstituteDistrict' column.")

    df["local"] = (
        df["district"].fillna("").str.lower().str.strip() ==
        df["InstituteDistrict"].fillna("").str.lower().str.strip()
    )

    return df


    