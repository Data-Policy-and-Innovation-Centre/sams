from datetime import datetime
from loguru import logger
from sams.config import GEOCODES, GEOCODES_CACHE, gmaps_geocode, novatim_geocode
import pandas as pd
import os
import time
import re
from tqdm import tqdm
from geopy.exc import GeocoderUnavailable, GeocoderQuotaExceeded, GeocoderTimedOut
from geopy import Location
import geopandas as gpd
import pickle
from rapidfuzz import process, fuzz
from base64 import b64decode
from Crypto.Cipher import AES


def save_data(df: pd.DataFrame, metadata: dict):
    """
    Saves a pandas DataFrame to a file based on the file type.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to be saved.
    metadata : dict
        A dictionary containing "path" and "type" keys. The path is the location
        where the data should be saved, and the type is one of "csv", "excel",
        "parquet", "json", or "feather".

    Returns
    -------
    None
    """
    
    path = metadata["path"]
    file_type = metadata["type"]

    if file_type == "csv":
        df.to_csv(path, index=False)
    elif file_type == "excel":
        df.to_excel(path, index=False)
    elif file_type == "parquet":
        df.to_parquet(path, index=False)
    elif file_type == "json":
        df.to_json(path, orient="records")
    elif file_type == "feather":
        df.to_feather(path)
    else:
        raise ValueError(f"Invalid file type: {file_type}")

    logger.info(f"Data saved to {path}")

def load_data(metadata: dict) -> pd.DataFrame:
    """
    Loads a pandas DataFrame from a file based on the file type.

    Parameters
    ----------
    metadata : dict
        A dictionary containing "path" and "type" keys. The path is the location
        where the data should be loaded from, and the type is one of "csv", "excel",
        "parquet", "json", "feather", or "shapefile".

    Returns
    -------
    pd.DataFrame
        The loaded DataFrame.
    """
    path = metadata["path"]
    filetype = metadata["type"]
    logger.info(f"Loading data from {path}")

    if filetype == "csv":
        return pd.read_csv(path)
    elif filetype == "excel":
        return pd.read_excel(path)
    elif filetype == "parquet":
        return pd.read_parquet(path)
    elif filetype == "json":
        return pd.read_json(path, orient="records")
    elif filetype == "feather":
        return pd.read_feather(path)
    elif filetype == "shapefile":
        return gpd.read_file(path)
    else:
        raise ValueError(f"Invalid file type: {filetype}")
        

def geocode(addr: str, google_maps: bool) -> Location | None:
    """
    Geocode an address using either the Google Maps API or Nominatim geocoder.

    Parameters
    ----------
    addr : str
        The address to geocode
    google_maps : bool
        Whether to use the Google Maps API or Nominatim

    Returns
    -------
    Location | None
        A geopy Location object if the address could be geocoded, otherwise None
    """

    if len(GEOCODES) % 100 == 0:
        with open(GEOCODES_CACHE, "wb") as f:
            pickle.dump(GEOCODES, f)

    if addr in GEOCODES:
        return GEOCODES[addr]
    else:
        try:
            if google_maps:
                location = gmaps_geocode(f"{addr}, India")
            else:
                location = novatim_geocode(f"{addr}, India")
            GEOCODES[addr] = location
            return location
        except (GeocoderUnavailable, GeocoderQuotaExceeded, GeocoderTimedOut) as e:
            logger.error(f"Error geocoding address {addr})")
            return None
        except Exception as e:
            logger.error(f"Error geocoding address {addr}: {e}")
            return None

def is_valid_date(date_string: str) -> tuple[bool, datetime | None]:
    """
    Checks if a given string can be parsed as a valid date in one of several formats.

    Parameters
    ----------
    date_string : str
        The string to check

    Returns
    -------
    tuple[bool, datetime | None]
        A tuple containing a boolean indicating whether the string can be parsed as a valid date,
        and the parsed date if it is valid, or None if it is not
    """
    formats = [
        "%Y-%m-%d",  # Format 1: 2024-08-26
        "%d-%m-%Y",  # Format 2: 26-08-2024
        "%m/%d/%Y",  # Format 3: 08/26/2024
        "%d %b %Y",  # Format 4: 26 Aug 2024
        "%B %d, %Y",  # Format 5: August 26, 2024
        "%Y-%m-%d %H:%M:%S",  # Format 6: 2024-08-26 15:30:00
        # Add more formats as needed
    ]

    for fmt in formats:
        try:
            parsed_date = datetime.strptime(date_string, fmt)
            return True, parsed_date  # Date is valid, return the parsed date
        except ValueError:
            continue  # Try the next format

    return False, None  # No formats matched, date is invalid


def camel_to_snake_case(text: str) -> str:
    # Step 0: All caps to be converted to lower case
    """
    Converts a given string from CamelCase to snake_case.

    Parameters
    ----------
    text : str
        The string to convert

    Returns
    -------
    str
        The converted string

    Notes
    -----
    This function has three steps:

    1. If the string is all uppercase, convert it to lowercase.
    2. Split the string by underscores, since underscores are already intentional.
    3. Convert each part to snake case if it's a CamelCase word by inserting underscores before
       each uppercase letter that is not the first letter of the part, and then lowercasing the
       entire part.

    Examples
    --------
    >>> camel_to_snake_case("CamelCase")
    "camel_case"
    >>> camel_to_snake_case("ALLCAPS")
    "allcaps"
    """
    if text.isupper():
        text = text.lower()

    text = correct_spelling(text)

    # Step 1: Split the text by underscores, since underscores are already intentional
    parts = text.split("_")

    # Step 2: Convert each part to snake case if it's a CamelCase word
    def convert_part(part):
        if re.search(r"[A-Z]", part):
            return re.sub(r"(?<!^)(?=[A-Z][a-z])", "_", part).lower()
        return part.lower()

    # Step 3: Join the converted parts back with underscores
    return "_".join(convert_part(part) for part in parts)


def dict_camel_to_snake_case(d: dict) -> dict:
    return {camel_to_snake_case(k): v for k, v in d.items()}


def correct_spelling(text: str) -> str:
    """Correct minor spelling errors in strings.

    This function corrects a small number of common spelling errors that are present
    in the data. It is not intended to be a comprehensive spelling corrector, but
    rather a small number of one-off corrections that are known to be present in the
    data.

    Notes
    -----
    This function is case-insensitive, and will correct spelling errors regardless
    of the case of the input string.

    Examples
    --------
    >>> correct_spelling("Tength")
    "Tenth"
    >>> correct_spelling("tength")
    "tenth"
    >>> correct_spelling("OR")
    "Or"
    >>> correct_spelling("TypeofInstitute")
    "type_of_institute"
    >>> correct_spelling("cuttoff")
    "cutoff"
    """
    
    if "Tength" in text or "tength" in text:
        text = text.replace("Tength", "Tenth").replace("tength", "tenth")
    if "OR" in text:
        text = text.replace("OR", "Or")
    if text == "TypeofInstitute":
        text = "type_of_institute"
    if text == "cuttoff":
        text = "cutoff"
    return text

# def correct_district_name(text: str) -> str:

#     if text == "Anugul":
#         return "Angul"
#     elif text == "Baleswar":
#         return "Balasore"
#     elif text == "Jagatsingpur":
#         return "Jagatsinghpur"
#     elif text == "Kendrapara":
#         return "Kendra"


def stop_logging_to_console(filename: str, mode: str = "a"):
    """
    Stops logging messages to the console and redirects them to a file.

    This function removes all existing logging handlers, effectively stopping
    any logging to the console. It then adds a new logging handler that writes
    log messages to the specified file. This is useful for capturing log
    messages in a file instead of displaying them in the console.

    Parameters
    ----------
    filename : str
        The path of the file where log messages should be written.
    mode : str, optional
        The mode in which the file is opened. Default is "a", which means
        append mode. Use "w" for write mode to overwrite the file.
    """
    for handler_id in list(logger._core.handlers.keys()):
        logger.remove(handler_id)

    # Add new logger
    logger.add(
        filename,
        format="{time} {level} {message}",
        level="INFO",
        colorize=True,
        catch=True,
        mode=mode,
    )


def resume_logging_to_console():
    """
    Resumes logging messages to the console using tqdm for writing.

    This function adds a new logging handler that writes log messages to the
    console. The messages are displayed using tqdm's write function, which is
    useful for keeping log messages separate from progress bar outputs.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)


def find_null_column(text: str):
    """
    Extracts the column name from a given string if it matches a specific pattern.

    Parameters
    ----------
    text : str
        The string to search for a pattern.

    Returns
    -------
    str or None
        The column name extracted from the string if the pattern matches,
        otherwise None.
    """
    match = re.search(r"(students)\.(\w+)|(institutes)\.(\w+)", text)
    return match.group().split(".")[1] if match else None


def hours_since_creation(path: str):
    """
    Calculate the number of hours since a file was last modified.

    Parameters
    ----------
    path : str
        The file path for which to calculate the hours since last modification.

    Returns
    -------
    float
        The number of hours since the file was last modified. If the file does 
        not exist, returns infinity (float("inf")).
    """
    if os.path.exists(path):
        return (time.time() - os.path.getmtime(path)) / 3600
    return float("inf")


def flatten(nested_list: list):
    """
    Flatten a nested list into a single list.

    Args:
        nested_list (list): List containing sublists.

    Returns:
        list: Single list containing all elements from the sublists.
    """
    return [item for sublist in nested_list for item in sublist]

def best_fuzzy_match(
    string: str, choices: list[str], threshold: float = 80
) -> str:
    """
    Find the best fuzzy match for a given string from a list of choices.

    Parameters
    ----------
    string : str
        The string to find the best match for.
    choices : list[str]
        The list of strings to search through for the best match.
    threshold : float, optional
        The minimum score for a match to be considered the best match. The default is 80.

    Returns
    -------
    str
        The best match from the list of choices.
    """
    match, score, _ =  process.extractOne(string, choices, scorer=fuzz.ratio, score_cutoff=0)
    return match if score >= threshold else None

def _group_dict(df: pd.DataFrame, group_by: list[str]) -> dict:
    """
    Convert a grouped DataFrame into a dictionary of DataFrames.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to group.
    group_by : list[str]
        Columns to group the DataFrame by.

    Returns
    -------
    dict
        A dictionary mapping the group names to the corresponding DataFrames.
    """
    groups = {}

    grouped_df = df.groupby(group_by)

    for group, group_df in grouped_df:
        groups[group] = group_df

    return groups

def _best_fuzzy_match_group(row: pd.Series, grouped_df: dict, exact_on: list[str], fuzzy_on: str, threshold: float = 80) -> pd.DataFrame:
    """
    Perform a fuzzy match on a grouped DataFrame.

    Parameters
    ----------
    row : pd.Series
        The row to perform the fuzzy match on.
    grouped_df : dict
        The grouped DataFrame to search.
    exact_on : list[str]
        The columns to group the DataFrame by.
    fuzzy_on : str
        The column to search for the best match.
    threshold : float, optional
        The minimum score for a match to be considered the best match. The default is 80.

    Returns
    -------
    str
        The best match from the list of choices.
    """
    try:
        return best_fuzzy_match(row[fuzzy_on], grouped_df[tuple(row[exact_on])][fuzzy_on].tolist(), threshold)
    except KeyError:
        return None

def fuzzy_merge(df1: pd.DataFrame, df2: pd.DataFrame, how: str, fuzzy_on: str, exact_on: list[str] = [], threshold: float = 70) -> pd.DataFrame | gpd.GeoDataFrame:
    """
    Merge two dataframes using fuzzy matching on specific columns.

    Parameters
    ----------
    df1 : pd.DataFrame
        The first dataframe to merge.
    df2 : pd.DataFrame
        The second dataframe to merge.
    how : str
        The merge method to use.
    fuzzy_on : str
        The column to merge on using fuzzy matching.
    exact_on : list[str], optional
        The columns to merge exactly on.
    threshold : float, optional
        The minimum score for a match to be considered the best match. The default is 80.

    Returns
    -------
    pd.DataFrame
        The merged dataframe.
    """
    df1 = df1.copy()
    if exact_on != []:
        grouped_df = _group_dict(df2, exact_on)
        df1[f"{fuzzy_on}_match"] = df1.apply(lambda row: _best_fuzzy_match_group(row, grouped_df, exact_on, fuzzy_on, threshold), axis=1)
    else:
        df1[f"{fuzzy_on}_match"] = df1.apply(lambda row: best_fuzzy_match(row[fuzzy_on], df2[fuzzy_on].tolist(), threshold), axis=1)
    df1 = df1.drop(columns=fuzzy_on)
    left_on = exact_on + [f"{fuzzy_on}_match"]
    right_on = exact_on + [fuzzy_on]
    return pd.merge(df1, df2, how=how, left_on=left_on, right_on=right_on)


def decrypt_roll(enc_text: str, key: bytes = b"y6idXfCVRG5t2dkeBnmHy9jLu6TEn5Du") -> str | None:
    """
    Decrypts an encrypted roll number.

    This function takes a Base64-encoded string that was encrypted using AES
    (ECB mode) and returns the original roll number as a readable string. 
    If decryption fails or the input is invalid, it returns None.

    Args:
        enc_text (str): Encrypted roll number (Base64-encoded).
        key (bytes, optional): AES encryption key. Defaults to a fixed key.

    Returns:
        str | None: Decrypted roll number, or None if invalid or decryption fails.
    """
    try:
        # Return None if input is empty or not a string
        if not enc_text or not isinstance(enc_text, str):
            return None

        # Decode Base64 to get the encrypted bytes
        raw = b64decode(enc_text)

        # Initialize AES cipher in ECB mode
        cipher = AES.new(key, AES.MODE_ECB)

        # Decrypt the bytes
        decrypted = cipher.decrypt(raw)

        # Get padding length from the last byte and validate
        pad_len = decrypted[-1]
        if pad_len < 1 or pad_len > 16:
            return None

        # Remove padding
        decrypted = decrypted[:-pad_len]

        # Convert bytes to string and strip whitespace
        roll_no = decrypted.decode("utf-8").strip()
        return roll_no

    except Exception:
        # Return None if any error occurs (decoding, decryption, etc.)
        return None


if __name__ == "__main__":
    from sams.config import datasets
    df = load_data(datasets["iti_enrollments"])
    print(_group_dict(df, ["district"]))
