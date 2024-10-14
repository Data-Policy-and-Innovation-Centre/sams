import pandas as pd
import re
import numpy as np


def _make_date(x: pd.Series) -> pd.Series:
    return pd.to_datetime(x, errors="coerce")


def _make_null(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace({"": np.nan, " ": np.nan, "NA": np.nan})


def _make_bool(x: pd.Series, true_val: str = "Yes", false_val: str = "No") -> pd.Series:
    return x.map({true_val: True, false_val: False})


def _fix_qual_names(x: pd.Series) -> pd.Series:
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


def _make_float(x: pd.Series) -> pd.Series:
    pass


def _make_int(x: pd.Series) -> pd.Series:
    pass


def preprocess_iti_students_enrolmentdata(df: pd.DataFrame) -> pd.DataFrame:
    df = _make_null(df)
    df["dob"] = _make_date(df["dob"])
    df["highest_qualification"] = _fix_qual_names(df["highest_qualification"])
    return df


def preprocess_students_markdata(df: pd.DataFrame) -> pd.DataFrame:
    pass


def preprocess_institutes(df: pd.DataFrame) -> pd.DataFrame:
    pass
