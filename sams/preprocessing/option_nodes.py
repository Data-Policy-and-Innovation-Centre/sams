import pandas as pd
import sqlite3
import json


def _make_null(df: pd.DataFrame) -> pd.DataFrame:
    """Replace empty strings, spaces, 'NA' with NaN"""
    return df.replace({"": np.nan, " ": np.nan, "NA": np.nan})

# def _make_date(x: pd.Series) -> pd.Series:
#     """Convert column to datetime"""
#     return pd.to_datetime(x, errors="coerce")


def option_data(db_path):
    """
    Reads the 'students' table from the SQLite database at db_path and returns it as a pandas DataFrame.
    """
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM students", conn)
    conn.close()
    return df

def extract_and_flatten_option_data(df, option_column='option_data'):
    """
    Extracts and flattens JSON option data from the specified column in the DataFrame.
    Returns a new DataFrame with the flattened option data.
    """
    if option_column in df.columns:
        option_df = df[option_column].apply(lambda x: json.loads(x) if pd.notnull(x) else {})
        option_flat = pd.json_normalize(option_df)
        result = pd.concat([df.drop(columns=[option_column]), option_flat], axis=1)
        return result
    else:
        return df

def analyze_applications(df):
    """
    Analyzes the number of students who applied and got reported.
    Returns a summary dictionary.
    """
    total_applied = len(df)
    total_reported = df['reported_institute'].notnull().sum()
    return {
        'total_applied': total_applied,
        'total_reported': total_reported
    }

def extract_first_choice(option_data_series: pd.Series) -> pd.DataFrame:
    """
    Extract first choice (Option_No == "1") from option_data JSON column.

    Parameters
    ----------
    option_data_series : pd.Series
        Series containing option_data JSON strings.

    Returns
    -------
    pd.DataFrame
        One row per student with first choice info.
    """
    first_choices = []

    for options_json in option_data_series:
        try:
            options = json.loads(options_json)
            first_choice = next((opt for opt in options if opt["Option_No"] == "1"), None)

            if first_choice:
                first_choices.append(first_choice)
            else:
                first_choices.append({})
        except json.JSONDecodeError:
            first_choices.append({})

    df = pd.DataFrame(first_choices)
    return df
