from pathlib import Path
import sqlite3
import pandas as pd
from loguru import logger
from hamilton.function_modifiers import (
    parameterize,
    source,
    value,
    cache,
)

from sams.config import PROJ_ROOT, LOGS, SAMS_DB
from sams.etl.orchestrate import SamsDataOrchestrator
from sams.etl.extract import SamsDataDownloader
from sams.utils import hours_since_creation
from sams.preprocessing.option_nodes import preprocess_option_data


# ========== Database Connection ========== #
@cache(behavior="DISABLE")
def sams_db(build: bool = True) -> sqlite3.Connection:
    """
    Establish a connection to the SAMS SQLite database.
    Optionally builds it by downloading data via the orchestrator.
    """
    if Path(SAMS_DB).exists() and not build:
        logger.info(f"Using existing database at {SAMS_DB}")
        return sqlite3.connect(SAMS_DB)

    if build:
        logger.info(f"Building SAMS database at {SAMS_DB} from API...")

        if not (PROJ_ROOT / ".env").exists():
            raise FileNotFoundError(f".env file not found at {PROJ_ROOT}/.env")

        downloader = SamsDataDownloader()

        if max(
            hours_since_creation(Path(LOGS / "students_count.csv")),
            hours_since_creation(Path(LOGS / "institutes_count.csv")),
        ) > 24:
            logger.info("Refreshing total records...")
            downloader.update_total_records()

        orchestrator = SamsDataOrchestrator(db_url=f"sqlite:///{SAMS_DB}")
        orchestrator.process_data("institutes", exclude=True, bulk_add=True)
        orchestrator.process_data("students", exclude=True, bulk_add=True)

        return sqlite3.connect(SAMS_DB)

    raise FileNotFoundError(f"SAMS DB not found at {SAMS_DB}")


# ========== Load Raw Student Data ========== #
@parameterize(
    iti_raw=dict(sams_db=source("sams_db"), module=value("ITI")),
    diploma_raw=dict(sams_db=source("sams_db"), module=value("Diploma")),
)
@cache(behavior="DISABLE")
def sams_students_raw_df(sams_db: sqlite3.Connection, module: str) -> pd.DataFrame:
    """
    Load student records from the SAMS DB by module (e.g., ITI, Diploma).
    """
    logger.info(f"Loading {module} students from SAMS DB")
    query = f"SELECT * FROM students WHERE module = '{module}'"
    return pd.read_sql_query(query, sams_db)


# ========== Preprocessing: Flatten & Classify Option Data ========== #
@parameterize(
    iti_options_flat=dict(sams_students_raw_df=source("iti_raw")),
    diploma_options_flat=dict(sams_students_raw_df=source("diploma_raw")),
)
def option_data_flattened(sams_students_raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten option_data JSON and classify institute type (Govt./Pvt./Mix).
    """
    logger.info("Flattening & classifying student option data...")
    return preprocess_option_data(sams_students_raw_df)


# ========== (Optional) Summary Stats ========== #
def summary_option_data(option_data_flattened: pd.DataFrame) -> pd.DataFrame:
    """
    Generate summary statistics from flattened option data.
    """
    logger.info("Computing summary statistics...")
    summary = (
        option_data_flattened.groupby(["academic_year", "module"])
        .agg(
            applications=("aadhar_no", "count"),
            unique_applicants=("aadhar_no", pd.Series.nunique),
            reported=("status", lambda x: (x == "Reported").sum())
        )
        .reset_index()
    )
    summary["apps_per_student"] = (
        summary["applications"] / summary["unique_applicants"]
    )
    return summary
