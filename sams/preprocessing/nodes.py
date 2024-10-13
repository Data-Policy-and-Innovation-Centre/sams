import pandas as pd


def _make_date(x: pd.Series) -> pd.Series:
    return pd.to_datetime(x, errors="coerce")

def _make_null(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace({"": None, " ": None, "NA": None})

def _make_bool(x: pd.Series, true_val: str = "Yes", false_val: str = "No") -> pd.Series:
    return x.map({true_val: True, false_val: False})

def _fix_qual_names(x: pd.Series) -> pd.Series:
    degree_names = [
        "ba",
        "ma",
        

    ]
    x = x.str.lower()
    x = x.apply(lambda x: "Diploma" if "Diploma" in x else x)
    return x


def _make_float(x: pd.Series) -> pd.Series:
    pass

def _make_int(x: pd.Series) -> pd.Series:
    pass

def preprocess_students_enrolmentdata(df: pd.DataFrame) -> pd.DataFrame:
    pass 

def preprocess_students_markdata(df: pd.DataFrame) -> pd.DataFrame:
    pass

def preprocess_institutes(df: pd.DataFrame) -> pd.DataFrame:
    pass



