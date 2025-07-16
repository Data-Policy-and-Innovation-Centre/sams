import pandas as pd
import numpy as np
import json
from sams.utils import dict_camel_to_snake_case, flatten, geocode
from loguru import logger
from sams.config import GEOCODES, GEOCODES_CACHE
import pickle
from tqdm import tqdm
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from geopy.distance import geodesic
import time

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


def _lat_long(df: pd.DataFrame, noisy: bool = True) -> pd.DataFrame:
    """
    Geocode latitude and longitude from block, district, and state columns with fallbacks.

    Tries in order:
        1. "block, district, state, India"
        2. "block, state, India"
        3. "block, India"
        4. "district, state, India"
        5. "district, India"

    Parameters
    ----------
    df : pd.DataFrame
        Must contain columns: 'block', 'district', 'state'
    noisy : bool
        Print geocoding progress

    Returns
    -------
    pd.DataFrame
        With 'latitude', 'longitude', 'used_address', 'geocode_level'
    """
    geolocator = Nominatim(user_agent="hss-geocoder")
    df = df.copy()

    def build_attempts(row):
        block = str(row.get("block", "") or "").strip()
        district = str(row.get("district", "") or "").strip()
        state = str(row.get("state", "") or "").strip()

        return [
            (f"{block}, {district}, {state}, India", "block+district+state"),
            (f"{block}, {state}, India", "block+state"),
            (f"{block}, India", "block"),
            (f"{district}, {state}, India", "district+state"),
            (f"{district}, India", "district")
        ]

    # Build all attempts
    df["attempts"] = df.apply(build_attempts, axis=1)

    # Flatten unique address attempts
    unique_attempts = {}
    for attempt_list in df["attempts"]:
        for addr, level in attempt_list:
            if addr not in unique_attempts:
                unique_attempts[addr] = level

    if noisy:
        print(f"[Geocoding] Unique fallback addresses to try: {len(unique_attempts)}")

    # Geocode each unique address
    locations = {}
    for addr in tqdm(unique_attempts, desc="Geocoding address variants"):
        try:
            location = geolocator.geocode(addr, timeout=10)
            if location:
                locations[addr] = (location.latitude, location.longitude)
        except GeocoderTimedOut:
            continue
        time.sleep(1)

    # Resolve for each row
    def resolve_row(attempts):
        for addr, level in attempts:
            if addr in locations:
                lat, lon = locations[addr]
                return pd.Series([lat, lon, addr, level])
        return pd.Series([np.nan, np.nan, None, None])

    df[["latitude", "longitude", "used_address", "geocode_level"]] = df["attempts"].apply(resolve_row)

    return df[["latitude", "longitude", "used_address", "geocode_level"]]


def _get_distance(coord_1: tuple, coord_2: tuple) -> float:
    """
    Computes geodesic distance between two coordinate points (lat/lng).

    Parameters
    ----------
    coord_1, coord_2 : tuples of (latitude, longitude)

    Returns
    -------
    float : distance in kilometers
    """
    try:
        return geodesic(coord_1, coord_2).kilometers
    except:
        return None
    

def preprocess_distances(df: pd.DataFrame) -> pd.DataFrame:
    df["distance"] = df.apply(
        lambda row: _get_distance((row["student_lat"], row["student_long"]),
                                  (row["institute_lat"], row["institute_long"])),
        axis=1
    )
    return df

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

    # Geocode address
    if all(col in df.columns for col in ["block", "district", "state"]):
        geocoded_df = _lat_long(df[["block", "district", "state"]].copy())
        df = pd.concat([df.reset_index(drop=True), geocoded_df.reset_index(drop=True)], axis=1)

    return df


def _preprocess_income_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    doc-string
    """
    if "annual_income" in df.columns:
        df["annual_income"] = df["annual_income"].astype(str).str.strip()
    return df


def extract_hss_options(df: pd.DataFrame, id_col: str = "barcode", option_col: str = "hss_option_details") -> pd.DataFrame:
    """
    Explodes the 'hss_option_details' JSON array into long-format rows per student.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with at least [student_id, hss_option_details] columns.
    id_col : str
        Column name for the student ID (used to track rows).
    option_col : str
        Column name containing the JSON string or list of options.

    Returns
    -------
    pd.DataFrame
        Flattened DataFrame with one row per option, including student ID.
    """
    records = []

    for _, row in df.iterrows():
        student_id = row.get(id_col)
        options_raw = row.get(option_col)

        # Parse safely
        if pd.isna(options_raw):
            continue
        try:
            options = json.loads(options_raw) if isinstance(options_raw, str) else options_raw
            for opt in options:
                opt = opt.copy()
                opt[id_col] = student_id
                records.append(opt)
        except Exception as e:
            # Optionally log or print parsing errors
            continue

    return pd.DataFrame.from_records(records)


def extract_hss_compartments(df: pd.DataFrame, compartment_col: str = "hss_compartments", id_col: str = "barcode") -> pd.DataFrame:
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
        raw = row.get(compartment_col)

        if pd.isna(raw):
            records.append({"COMPSubject": None, "COMPFailMark": None, "COMPPassMark": None, id_col: barcode})
            continue

        try:
            compartments = json.loads(raw)

            if isinstance(compartments, list) and compartments:
                for subject in compartments:
                    record = subject.copy()
                    record[id_col] = barcode
                    records.append(record)
            else:
                records.append({"COMPSubject": None, "COMPFailMark": None, "COMPPassMark": None, id_col: barcode})

        except Exception:
            records.append({"COMPSubject": None, "COMPFailMark": None, "COMPPassMark": None, id_col: barcode})

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
    # Core cleaning
    df = _preprocess_hss_students(df, geocode=False)

    # Drop unnecessary columns if present
    drop_cols = [
        "student_name",
        "nationality",
        "address",
        "pin_code",
        "board_exam_namefor_highest_qualification",
        "examination_type",
        "examination_boardofthe_highest_qualification",
        "national_cadet_corps",
        "orphan",
        "sports", 
        "year_of_passing", 
        "hss_option_details",  # flatten separately if needed
        "hss_compartments",  # handle separately
    ]
    df = df.drop([col for col in drop_cols if col in df.columns], axis=1)

    # Clean income column if it exists
    if "annual_income" in df.columns:
        df = _preprocess_income_data(df)

    sort_cols = [col for col in ["barcode", "academic_year"] if col in df.columns]
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


def merge_institute_geocodes(admitted_df: pd.DataFrame, geocode_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge student admission records with institute geocodes using SAMSCode.

    Parameters
    ----------
    admitted_df : pd.DataFrame
        Student-level data with SAMSCode from the admitted option.
    geocode_df : pd.DataFrame
        Institute geocodes with columns: SAMSCode, latitude, longitude.

    Returns
    -------
    pd.DataFrame
        Student data with `institute_lat`, `institute_long` columns.
    """
    if "SAMSCode" not in admitted_df.columns:
        raise ValueError("Missing 'SAMSCode' column. Run extract_priority_hss_option() first.")

    merged = pd.merge(admitted_df, geocode_df, how="left", on="SAMSCode")

    merged.rename(columns={
        "latitude": "institute_lat",
        "longitude": "institute_long"
    }, inplace=True)

    return merged


def preprocess_distances(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a 'distance' column with geodesic distance (in km) between student and institute coordinates.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain: student_lat, student_long, institute_lat, institute_long

    Returns
    -------
    pd.DataFrame
        With new 'distance' column added (NaN if coordinates missing)
    """
    required_cols = ["student_lat", "student_long", "institute_lat", "institute_long"]
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"Missing one of required columns: {required_cols}")

    df["distance"] = df.apply(
        lambda row: _get_distance(
            (row["student_lat"], row["student_long"]),
            (row["institute_lat"], row["institute_long"])
        ) if not pd.isnull(row["student_lat"]) and not pd.isnull(row["institute_lat"]) else None,
        axis=1
    )

    return df


def _get_distance(coord_1: tuple, coord_2: tuple) -> float:
    try:
        return geodesic(coord_1, coord_2).kilometers
    except ValueError as e:
        return None
    
    