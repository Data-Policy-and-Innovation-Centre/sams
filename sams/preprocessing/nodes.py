import pandas as pd
import numpy as np
import json
from sams.util import dict_camel_to_snake_case, flatten, geocode
from loguru import logger
from sams.config import GEOCODES, GEOCODES_CACHE
import pickle


def _make_date(x: pd.Series) -> pd.Series:
    """
    Convert a pandas Series of dates to datetime64[ns] type

    Parameters
    ----------
    x : pd.Series
        Series of dates

    Returns
    -------
    pd.Series
        Series of datetime64[ns] type
    """
    return pd.to_datetime(x, errors="coerce")


def _lat_long(df: pd.DataFrame, address_col: str = "pin_code", noisy: bool = True) -> pd.DataFrame:
    """
    Create longitude and latitude columns from pin_code column in a DataFrame

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with pin_code column
    noisy : bool, optional
        Print logs to console, by default False

    Returns
    -------
    pd.DataFrame
        DataFrame with longitude and latitude columns
    """
    addresses = df[address_col].drop_duplicates()
    if noisy:
        logger.info(f"Number of unique addresses: {len(addresses)}")

    locations = {addr: geocode(f"{addr}") for addr in addresses}
    locations = {addr: loc for addr, loc in locations.items() if loc is not None}

    if noisy:
        logger.info(f"Number of successfully geocoded addresses: {len(locations)}")

    df["longitude"] = df[address_col].map(
        lambda addr: locations[addr].longitude if addr in locations.keys() else np.nan
    )
    df["latitude"] = df[address_col].map(
        lambda addr: locations[addr].latitude if addr in locations.keys() else np.nan
    )
    return df


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


def _fix_qual_names(x: pd.Series) -> pd.Series:
    """
    Standardize highest qualification names

    Parameters
    ----------
    x : pd.Series
        Series of highest qualification names

    Returns
    -------
    pd.Series
        Standardized highest qualification names

    Notes
    -----
    This function maps the highest qualification names to standardized categories
    using aggregations and regular expressions. Categories are:

    - "graduate and above"
    - "diploma"
    - "12th"
    - "10th"

    Examples
    --------
    >>> _fix_qual_names(pd.Series(["BA", "Diploma", "12th", "10th"]))
    0    graduate and above
    1          diploma
    2           12th
    3           10th
    dtype: object
    """

    degree_names = [
        "ba",
        "ma",
        "bped",
        "mtech",
        "btech",
        "bsc",
        "msc",
        "llb",
        "mca",
        "pgdca",
        "blib",
        "mlib",
        "bca",
        "bba",
        "mba",
        "bcom",
        "mcom",
        "bed",
        "med",
        "graduation",
        "bcam",
        "pg",
        "post graduate",
    ]
    diploma_names = ["diploma", "jbt/ett"]

    # Standardize format
    x = x.str.lower()
    x = x.str.replace(".", "")
    x = x.str.replace(r"\(.*?\)", "", regex=True)
    x = x.str.strip()

    # Standardize qual names using aggregations
    x = x.apply(lambda x: "graduate and above" if x in degree_names else x)
    x = x.apply(lambda x: "diploma" if x in diploma_names else x)

    # Fix qual with misc. and non-standard spellings
    x = x.apply(lambda x: "graduate and above" if "graduation" in str(x) else x)
    x = x.apply(lambda x: "graduate and above" if "degree" in str(x) else x)
    x = x.apply(lambda x: "diploma" if "diploma" in str(x) or "dped" in str(x) else x)
    x = x.apply(lambda x: "diploma" if "iti" in str(x) else x)
    x = x.apply(lambda x: "10th" if "10" in str(x) else x)
    x = x.apply(lambda x: "10th" if "matric" in str(x) else x)
    x = x.apply(lambda x: "10th" if "bse" in str(x) else x)
    x = x.apply(lambda x: "12th" if "hsc" in str(x) else x)
    x = x.apply(lambda x: "12th" if "chse" in str(x) else x)
    x = x.apply(lambda x: "12th" if "12" in str(x) else x)
    x = x.apply(lambda x: "12th" if "intermediate" in str(x) else x)
    x = x.apply(lambda x: "12th" if "intermedia" in str(x) else x)
    x = x.apply(lambda x: "12th" if "plus two" in str(x) or "plus 2" in str(x) else x)

    return x


def _extract_mark_data(x: pd.Series, key: str, value: str, varnames: list) -> pd.Series:
    """
    Extract columns from a pandas Series of json strings based on a condition

    Parameters
    ----------
    x : pd.Series
        Series of json strings
    key : str
        Key to filter by
    value : str
        Value of the key to filter by
    varnames : list
        List of column names to extract

    Returns
    -------
    pd.Series
        A Series containing the extracted columns
    """
    filtered_dfs = [
        pd.DataFrame(json.loads(marks))[lambda df: df[key] == value] for marks in x
    ]
    filtered_dfs = [
        (
            df
            if not df.empty
            else pd.DataFrame({col: np.nan for col in df.columns}, index=[0])
        )
        for df in filtered_dfs
    ]

    col = pd.concat(filtered_dfs)[varnames]
    col.reset_index(drop=True, inplace=True)
    return col


def _preprocess_students(df: pd.DataFrame, geocode=True) -> pd.DataFrame:
    """
    Preprocess student data

    Parameters
    ----------
    df : pd.DataFrame
        Student data
    geocode : bool, optional
        Whether to geocode pin_code column, by default True

    Returns
    -------
    pd.DataFrame
        Preprocessed student data

    Notes
    -----
    1. Replaces empty strings, spaces and "NA" with NaN
    2. Converts "dob" and "date_of_application" columns to datetime64[ns]
    3. Converts boolean columns to boolean type
    4. If geocode is True, creates "longitude" and "latitude" columns from pin_code
    """
    df = _make_null(df)
    df["dob"] = _make_date(df["dob"])
    df["date_of_application"] = _make_date(df["date_of_application"])
    bool_vars = [
        "had_two_year_full_time_work_exp_after_tenth",
        "gc",
        "ph",
        "es",
        "sports",
        "national_cadet_corps",
        "pm_care",
        "orphan",
    ]
    df[bool_vars] = df[bool_vars].apply(_make_bool)

    if geocode:
        df = _lat_long(df)

    return df


def preprocess_iti_students_enrollment_data(df: pd.DataFrame) -> pd.DataFrame:
    df = _preprocess_students(df, geocode=False)
    df["highest_qualification"] = _fix_qual_names(df["highest_qualification"])
    df = df.drop(
        [
            "student_name",
            "nationality",
            "domicile",
            "s_domicile_category",
            "outside_odisha_applicant_state_name",
            "odia_applicant_living_outside_odisha_state_name",
            "tenth_exam_school_address",
            "eighth_exam_school_address",
            "had_two_year_full_time_work_exp_after_tenth",
            "national_cadet_corps",
            "pm_care",
            "tfw",
        ],
        axis=1,
    )
    df.sort_values(by=["aadhar_no", "date_of_application"], inplace=True)
    df = df.dropna(subset=["sams_code"])
    return df


def preprocess_diploma_students_enrollment_data(df: pd.DataFrame) -> pd.DataFrame:
    df = _preprocess_students(df, geocode=False)
    df["tenth_passing_year"] = _extract_mark_data(
        df["mark_data"], "ExamName", "10th", ["YearofPassing"]
    )
    df = df.drop(
        [
            "student_name",
            "nationality",
            "domicile",
            "s_domicile_category",
            "highest_qualification",
            "outside_odisha_applicant_state_name",
            "odia_applicant_living_outside_odisha_state_name",
            "tenth_exam_school_address",
            "eighth_exam_school_address",
            "had_two_year_full_time_work_exp_after_tenth",
            "national_cadet_corps",
            "pm_care",
            "tfw",
        ],
        axis=1,
    )
    df.sort_values(by=["aadhar_no", "date_of_application"], inplace=True)
    df = df.dropna(subset=["sams_code"])
    return df


def preprocess_students_marks_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess students marks data

    Parameters
    ----------
    df : pd.DataFrame
        Students data

    Returns
    -------
    pd.DataFrame
        Preprocessed students marks data

    Notes
    -----
    1. Extract marks data from "mark_data" column
    2. Each mark is a dictionary with keys: "SubjectName", "TotalMarks", "SecuredMarks", "Percentage"
    3. Add "aadhar_no" and "academic_year" columns to the marks data
    4. Drop duplicate rows based on "aadhar_no" and "academic_year" columns, keeping the first occurrence
    """
    marks = [
        [
            dict_camel_to_snake_case(
                {**mark, "aadhar_no": aadhar, "academic_year": academic_year}
            )
            for mark in json.loads(marks)
        ]
        for aadhar, marks, academic_year in df[
            ["aadhar_no", "mark_data", "academic_year"]
        ].values
    ]

    marks = pd.DataFrame(flatten(marks))
    marks.drop_duplicates(
        subset=["aadhar_no", "academic_year"], keep="first", inplace=True
    )

    return marks


def extract_institute_strength(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract strength data from "strength" column

    Parameters
    ----------
    df : pd.DataFrame
        Institute data

    Returns
    -------
    pd.DataFrame
        Extracted strength data

    Notes
    -----
    1. Extract strength data from "strength" column
    2. Add "sams_code", "trade", "branch", "module", and "academic_year" columns to the strength data
    3. Melt the strength data into a long format with "category" and "strength" columns
    4. Remove "Strength" from the beginning of the category names
    5. Remove leading underscores from the category names
    """
    strength_df = pd.DataFrame(df["strength"].apply(json.loads).apply(pd.Series))
    strength_df = pd.concat([df[["sams_code", "trade", "branch", "module", "academic_year"]], strength_df], axis=1)
    strength_df = strength_df.melt(
        id_vars=["sams_code", "trade", "branch", "module", "academic_year"],
        var_name="category",
        value_name="strength",
    )
    strength_df["category"] = strength_df["category"].str.replace("Strength", "")
    strength_df["category"] = strength_df["category"].str.lstrip('_')
    
    return strength_df


def extract_institute_cutoffs(df: pd.DataFrame) -> pd.DataFrame:
    cutoffs_df = pd.DataFrame(df["cutoffs"].apply(json.loads).apply(pd.Series))
    cutoffs_df = cutoffs_df.apply(pd.to_numeric)
    cutoffs_df = pd.concat([df[["sams_code", "trade", "branch"]], cutoffs_df], axis=1)

def preprocess_iti_addresses(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=str.lower)
    df.columns = df.columns.str.replace(" ", "_")
    df = df[["iti_code", "address", "state", "district"]]
    df.rename(columns={"iti_code": "ncvtmis_code"}, inplace=True)
    df["address"] = df.apply(lambda row: f"{row['address']}, {row['district']}, {row['state']}", axis=1)
    return df

def preprocess_institutes(institutes_df: pd.DataFrame, ) -> pd.DataFrame:
    pass


def preprocess_geocodes(dfs: list[pd.DataFrame], address_col: list[str]) -> pd.DataFrame:
    """
    Concatenate the pincode columns of the input DataFrames and add longitude and latitude columns to the result

    Parameters
    ----------
    dfs : list[pd.DataFrame]
        List of DataFrames containing an address column
    address_col : list[str]
        The column name of the address

    Returns
    -------
    pd.DataFrame
        DataFrame with address, longitude and latitude columns
    """
    if len(dfs) != len(address_col):
        with open(GEOCODES_CACHE, "wb") as f:
            pickle.dump(GEOCODES, f)
        raise ValueError("The number of input DataFrames must be equal to the number of address columns")

    try:
        data = flatten([df[col].drop_duplicates().reset_index(drop=True).to_list() for df, col in zip(dfs,address_col)])
        df = pd.DataFrame({"address": data})
        df = _lat_long(df, address_col="address", noisy=True)
    except KeyError:
        logger.error("No 'pin_code' column found in input DataFrames")
    except Exception as e:
        # Change later
        logger.error(f"Some error occurred: {e}")
    finally:
        with open(GEOCODES_CACHE, "wb") as f:
            pickle.dump(GEOCODES, f)

    return df
