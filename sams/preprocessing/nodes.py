import pandas as pd
import re
import numpy as np
import json
from sams.util import dict_camel_to_snake_case


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
        "post graduate"
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
    # Use list comprehension to make this faster
    filtered_dfs = [
        pd.DataFrame(json.loads(marks))[lambda df: df[key] == value]
        for marks in x
    ]
    return pd.concat(filtered_dfs)[varnames]
   
    
def _preprocess_students(df: pd.DataFrame) -> pd.DataFrame:
    df = _make_null(df)
    df["dob"] = _make_date(df["dob"])
    df['date_of_application'] = _make_date(df['date_of_application'])
    bool_vars = ['had_two_year_full_time_work_exp_after_tenth', 'gc', 'ph', 'es',
       'sports', 'national_cadet_corps', 'pm_care', 'orphan']
    df[bool_vars] = df[bool_vars].apply(_make_bool)
    return df    

def preprocess_iti_students_enrollment_data(df: pd.DataFrame) -> pd.DataFrame:
    df = _preprocess_students(df)
    df["highest_qualification"] = _fix_qual_names(df["highest_qualification"])
    df = df.drop(["id","student_name", "nationality", "domicile", "s_domicile_category",
                  "outside_odisha_applicant_state_name","odia_applicant_living_outside_odisha_state_name","tenth_exam_school_address",
                  "eighth_exam_school_address","had_two_year_full_time_work_exp_after_tenth", "national_cadet_corps", "pm_care", "tfw"],axis=1)
    df = df.dropna(subset=["sams_code"])
    return df

def preprocess_diploma_students_enrollment_data(df: pd.DataFrame) -> pd.DataFrame:
    df = _preprocess_students(df)
    df['tenth_passing_year'] = _extract_mark_data(df['mark_data'], 'ExamName', '10th', ['YearofPassing'])
    df = df.drop(["id","student_name", "nationality", "domicile", "s_domicile_category", "highest_qualification",
                  "outside_odisha_applicant_state_name","odia_applicant_living_outside_odisha_state_name","tenth_exam_school_address",
                  "eighth_exam_school_address","had_two_year_full_time_work_exp_after_tenth", "national_cadet_corps", "pm_care", "tfw"],axis=1)
    df = df.dropna(subset='sams_code')
    return df
    

def preprocess_students_marks_data(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame([[dict_camel_to_snake_case({**mark, 'aadhar_no': aadhar, 'academic_year': academic_year}) for mark in json.loads(marks)] 
                         for aadhar, marks, academic_year in df[['aadhar_no','mark_data', 'academic_year']].values])



def preprocess_institutes(df: pd.DataFrame) -> pd.DataFrame:
    pass
