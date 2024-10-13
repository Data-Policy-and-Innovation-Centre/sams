from datetime import datetime
from loguru import logger
from sams.config import RAW_DATA_DIR, LOGS
import os
import time
import sqlite3
import pandas as pd
import re
from tqdm import tqdm


def is_valid_date(date_string):
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


def get_existing_modules(
    table_name: str = "students", db_path=f"{RAW_DATA_DIR}/sams.db"
) -> list:

    if table_name not in ["students", "institutes"]:
        raise ValueError(f"Invalid table name: {table_name}")

    counts_path = os.path.join(LOGS, table_name + "_count.csv")

    if not os.path.exists(db_path):
        return []

    if not os.path.exists(counts_path):
        return []

    counts = pd.read_csv(counts_path)

    if table_name == "students":
        query = """SELECT module, academic_year, COUNT(*) AS num_observations 
                    FROM students 
                    GROUP BY module, academic_year"""
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute(query)
            existing_modules = cur.fetchall()

        existing_modules = [list(row) for row in existing_modules]
        modules_with_duplicates = [
            (module, year, num_obs)
            for module, year, num_obs in existing_modules
            if num_obs
            > counts[(counts["module"] == module) & (counts["academic_year"] == year)][
                "count"
            ].values[0]
        ]
        if modules_with_duplicates:
            logger.warning(
                f"Modules with excess records than expected found: {modules_with_duplicates}"
            )
        existing_modules = [
            (module, year)
            for module, year, num_obs in existing_modules
            if num_obs
            >= counts[(counts["module"] == module) & (counts["academic_year"] == year)][
                "count"
            ].values[0]
        ]

        return existing_modules

    else:
        return []


def camel_to_snake_case(text: str):

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


def dict_camel_to_snake_case(d: dict):
    return {camel_to_snake_case(k): v for k, v in d.items()}


def correct_spelling(text: str):
    if "Tength" in text or "tength" in text:
        text = text.replace("Tength", "Tenth").replace("tength", "tenth")
    if "OR" in text:
        text = text.replace("OR", "Or")
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


if __name__ == "__main__":
    pass
