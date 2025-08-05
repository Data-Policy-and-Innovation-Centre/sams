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
)
from sams.config import LOGS, PROJ_ROOT, SAMS_DB, datasets
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
    hss_student_enrollments=dict(df=source("hss_raw")),
)
def preprocess_hss_enrollment(df: pd.DataFrame) -> pd.DataFrame:
    return preprocess_hss_students_enrollment_data(df)


# ===== Extract and Preprocess Marks =====
@parameterize(
    hss_student_marks=dict(hss_student_enrollments=source("hss_student_enrollments")),
)
def extract_preprocess_hss_marks(hss_student_enrollments: pd.DataFrame) -> pd.DataFrame:
    return preprocess_students_compartment_marks(hss_student_enrollments)


# ===== Flatten choice admitted students ======
@parameterize(
    hss_student_applications=dict(hss_raw=source("hss_raw")),
)
def flatten_student_options(hss_raw: pd.DataFrame) -> pd.DataFrame:
    enrollment = preprocess_hss_students_enrollment_data(hss_raw)
    return extract_hss_options(enrollment)


# ===== First choice admitted ======
@parameterize(
    hss_first_choice_admissions=dict(hss_student_applications=source("hss_student_applications")),
)
def filter_first_choice(hss_student_applications: pd.DataFrame) -> pd.DataFrame:
    return filter_admitted_on_first_choice(hss_student_applications)


# ===== Save Outputs =====
@save_to.parquet(path=value(datasets["hss_student_enrollments"]["path"]))
def save_hss_enrollments(hss_student_enrollments: pd.DataFrame) -> pd.DataFrame:
    return hss_student_enrollments

@save_to.parquet(path=value(datasets["hss_student_applications"]["path"]))
def save_hss_applications(hss_student_applications: pd.DataFrame) -> pd.DataFrame:
    return hss_student_applications


@save_to.parquet(path=value(datasets["hss_student_marks"]["path"]))
def save_hss_marks(hss_student_marks: pd.DataFrame) -> pd.DataFrame:
    return hss_student_marks


@save_to.parquet(path=value(datasets["hss_first_choice_admissions"]["path"]))
def save_hss_first_choice_admissions(hss_first_choice_admissions: pd.DataFrame) -> pd.DataFrame:
    return hss_first_choice_admissions
