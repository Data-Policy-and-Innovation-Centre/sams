from datetime import datetime
from loguru import logger
from sams.config import GEOCODES, gmaps_geocode, novatim_geocode
import pandas as pd
import os
import time
import re
from tqdm import tqdm
from geopy.exc import GeocoderUnavailable, GeocoderQuotaExceeded, GeocoderTimedOut
from geopy import Location


def save_data(df: pd.DataFrame, metadata: dict):
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
    else:
        raise ValueError(f"Invalid file type: {filetype}")
    
    

def geocode(addr: str, google_maps: bool) -> Location | None:
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
    if "Tength" in text or "tength" in text:
        text = text.replace("Tength", "Tenth").replace("tength", "tenth")
    if "OR" in text:
        text = text.replace("OR", "Or")
    if text == "TypeofInstitute":
        text = "type_of_institute"
    if text == "cuttoff":
        text = "cutoff"
    return text


def stop_logging_to_console(filename: str, mode: str = "a"):
    # Remove all handlers
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
    logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)


def find_null_column(text: str):
    match = re.search(r"(students)\.(\w+)|(institutes)\.(\w+)", text)
    return match.group().split(".")[1] if match else None


def hours_since_creation(path: str):
    if os.path.exists(path):
        return (time.time() - os.path.getmtime(path)) / 3600
    return float("inf")


def flatten(nested_list: list):
    return [item for sublist in nested_list for item in sublist]


if __name__ == "__main__":
    pass
