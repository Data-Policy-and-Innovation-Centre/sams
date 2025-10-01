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
from sams.config import LOGS, PROJ_ROOT, SAMS_DB, datasets
from sams.etl.extract import SamsDataDownloader
from sams.etl.orchestrate import SamsDataOrchestrator
from sams.utils import hours_since_creation, save_data
from sams.preprocessing.deg_nodes import (
    preprocess_deg_students_enrollment_data,
    preprocess_deg_options_details,
    preprocess_deg_compartments
)

# Build or Load SAMS Database 
# Build or Load SAMS Database 
@cache(behavior="DISABLE")
def sams_db(build: bool = True) -> sqlite3.Connection:
    if Path(SAMS_DB).exists() and not build:
        logger.info(f"Using existing database at {SAMS_DB}")
        return sqlite3.connect(SAMS_DB)

    if build:
        logger.info(f"Building database at {SAMS_DB} from SAMS API")

        if not Path(PROJ_ROOT / ".env").exists():
            raise FileNotFoundError(f".env file not found at {PROJ_ROOT}/.env")

        downloader = SamsDataDownloader()

        if (
            max(
                hours_since_creation(Path(LOGS / "students_count.csv")),
                hours_since_creation(Path(LOGS / "institutes_count.csv")),
            )
            > 24
        ):
            logger.info("Updating total records logs...")
            downloader.update_total_records()

        orchestrator = SamsDataOrchestrator(db_url=f"sqlite:///{SAMS_DB}")
        orchestrator.process_data("institutes", exclude=True, bulk_add=True)
        orchestrator.process_data("students", exclude=True, bulk_add=True)

        return sqlite3.connect(SAMS_DB)

    raise FileNotFoundError(f"Database not found at {SAMS_DB}")

# Load Raw DEG Student Data
@parameterize(
    deg_raw=dict(sams_db=source("sams_db"), module=value("DEG")),
)
@cache(behavior="DISABLE")
def deg_raw(sams_db: sqlite3.Connection, module: str) -> pd.DataFrame:
    logger.info(f"Loading raw {module} student data from database")

    query = """
        SELECT * 
        FROM students 
        WHERE module = ?;
    """
    df = pd.read_sql_query(query, sams_db, params=(module,))

    print(f"Loaded {len(df)} records for {module} across all years.")
    return df


# Preprocess DEG Enrollment Data 
@parameterize(
    deg_enrollments=dict(df=source("deg_raw")),
)
def preprocess_deg_enrollment(df: pd.DataFrame) -> pd.DataFrame:
    return preprocess_deg_students_enrollment_data(df)

# Preprocess DEG application data
@parameterize(
    deg_applications = dict(df = source ("deg_raw")),
)
def preprocess_deg_applications (df: pd.DataFrame) -> pd.DataFrame:
    return preprocess_deg_options_details(df)

# Preprocess DEG marks
@parameterize(
    deg_marks=dict(df=source("deg_raw")),
)
def preprocess_deg_marks(df: pd.DataFrame) -> pd.DataFrame:
    return preprocess_deg_compartments(df)

# save the nodes

@parameterize(
    save_deg_enrollments=dict(
        df=source("deg_enrollments"),
        dataset_key=value("deg_enrollments"),
    )
)
def save_deg_enrollments(df: pd.DataFrame, dataset_key: str) -> pd.DataFrame:
    """Saver for DEG enrollment data."""
    logger.info(f"Saving DEG data → {dataset_key}")
    save_data(df, datasets[dataset_key])
    return df


@parameterize(
    save_deg_applications=dict(
        df=source("deg_applications"),
        dataset_key=value("deg_applications"),
    )
)
def save_deg_applications(df: pd.DataFrame, dataset_key: str) -> pd.DataFrame:
    """Saver for DEG application data."""
    logger.info(f"Saving DEG data → {dataset_key}")
    save_data(df, datasets[dataset_key])
    return df

@parameterize(
    save_deg_marks=dict(
        df=source("deg_marks"),
        dataset_key=value("deg_marks"),
    )
)
def save_deg_marks(df: pd.DataFrame, dataset_key: str) -> pd.DataFrame:
    """Saver for DEG compartments / marks data."""
    logger.info(f"Saving DEG data → {dataset_key}")
    save_data(df, datasets[dataset_key])
    return df
