from pathlib import Path
import sqlite3
import pandas as pd
from loguru import logger
from hamilton.function_modifiers import (
    parameterize,
    source,
    value,
    save_to,
    cache,
    datasaver
    )
from hamilton.io import utils
from sams.config import LOGS, PROJ_ROOT, SAMS_DB, datasets, HSS_DATA_DIR
from sams.etl.extract import SamsDataDownloader
from sams.etl.orchestrate import SamsDataOrchestrator
from sams.utils import save_data, hours_since_creation, load_data
from sams.preprocessing.hss_nodes import (
    extract_hss_options,
    extract_hss_compartments,
    preprocess_students_compartment_marks,
    filter_admitted_on_first_choice,
    preprocess_hss_students_enrollment_data,
)


# ===== Building raw SAMS database from API =====
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

        # Update total record logs if older than 24 hours
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


# ===== Load Raw HSS Student Data =====
@parameterize(
    hss_raw=dict(sams_db=source("sams_db"), module=value("HSS")),
)
@cache(behavior="DISABLE")
def load_hss_students_raw(sams_db: sqlite3.Connection, module: str) -> pd.DataFrame:
    logger.info(f"Loading raw {module} student data from database")
    query = f"SELECT * FROM students WHERE module = '{module}';"
    return pd.read_sql_query(query, sams_db)


# ===== Preprocess Enrollment Data from Raw =====
@parameterize(
    hss_enrollment=dict(df=source("hss_raw")),
)
def preprocess_hss_enrollment(df: pd.DataFrame) -> pd.DataFrame:
    return preprocess_hss_students_enrollment_data(df)


# ===== Extract and Preprocess Marks =====
@parameterize(
    hss_marks=dict(hss_enrollment=source("hss_enrollment")),
)
def extract_preprocess_hss_marks(hss_enrollment: pd.DataFrame) -> pd.DataFrame:
    return preprocess_students_compartment_marks(hss_enrollment)

# ===== Extract Compartment Subject Info =====
@parameterize(
    hss_compartment_subjects=dict(hss_raw=source("hss_raw")),
)
def extract_compartments(hss_raw: pd.DataFrame) -> pd.DataFrame:
    enrollment = preprocess_hss_students_enrollment_data(hss_raw)
    return extract_hss_compartments(enrollment)

# ===== Flatten choice admitted students ======
@parameterize(
    flattened_hss_options=dict(hss_raw=source("hss_raw")),
)
def flatten_student_options(hss_raw: pd.DataFrame) -> pd.DataFrame:
    enrollment = preprocess_hss_students_enrollment_data(hss_raw)
    return extract_hss_options(enrollment)

# ===== First choice admitted ======
@parameterize(
    first_choice_admitted_students=dict(flattened_hss_options=source("flattened_hss_options")),
)
def filter_first_choice(flattened_hss_options: pd.DataFrame) -> pd.DataFrame:
    return filter_admitted_on_first_choice(flattened_hss_options)


# ===== Save HSS Enrollment =====
@save_to.parquet(path=value(datasets["hss_enrollment"]["path"]))
def save_hss_enrollment(hss_enrollment: pd.DataFrame) -> pd.DataFrame:
    return hss_enrollment

@save_to.parquet(path=value(datasets["hss_flattened_options"]["path"]))
def save_flattened_options(flattened_hss_options: pd.DataFrame) -> pd.DataFrame:
    return flattened_hss_options

@save_to.parquet(path=value(datasets["hss_compartment_subjects"]["path"]))
def save_compartment_subjects(hss_compartment_subjects: pd.DataFrame) -> pd.DataFrame:
    return hss_compartment_subjects

@save_to.parquet(path=value(datasets["first_choice_admitted"]["path"]))
def save_first_choice_admitted(first_choice_admitted_students: pd.DataFrame) -> pd.DataFrame:
    return first_choice_admitted_students

