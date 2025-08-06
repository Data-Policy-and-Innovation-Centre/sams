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
    datasaver,
)
from sams.config import LOGS, PROJ_ROOT, SAMS_DB, datasets
from sams import utils
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

# ===== Load Raw HSS Student Data (testing with 2018 only, 50k rows) =====
@parameterize(
    hss_raw=dict(sams_db=source("sams_db"), module=value("HSS")),
)
@cache(behavior="DISABLE")
def hss_raw(sams_db: sqlite3.Connection, module: str) -> pd.DataFrame:
    focus_year = 2018
    row_limit = 10000
    
    logger.info(f"Loading raw {module} student data from database (year={focus_year}, limit={row_limit})...")

    # Get available academic years (for info only)
    academic_year_query = "SELECT DISTINCT academic_year FROM students WHERE module = ?;"
    years = pd.read_sql_query(academic_year_query, sams_db, params=(module,))
    
    print(f"\n Starting to load raw {module} student data...")
    print(f" Found academic years for {module}: {list(years['academic_year'])}")

    # Focus on 2018 only with row limit
    query = f"""
        SELECT * 
        FROM students 
        WHERE module = ? AND academic_year = ? 
        LIMIT {row_limit};
    """
    df = pd.read_sql_query(query, sams_db, params=(module, focus_year))

    print(f"Loaded {len(df)} records for {module} in {focus_year} (limited to {row_limit}).")
    return df

# ===== Preprocess Enrollment Data from Raw =====
@parameterize(
    hss_enrollments=dict(df=source("hss_raw")),
)
def preprocess_hss_enrollment(df: pd.DataFrame) -> pd.DataFrame:
    return preprocess_hss_students_enrollment_data(df)


# ===== Extract and Preprocess Marks =====
@parameterize(
    hss_marks=dict(hss_raw=source("hss_raw")),
)
def extract_preprocess_hss_marks(hss_raw: pd.DataFrame) -> pd.DataFrame:
    return preprocess_students_compartment_marks(hss_raw)


# ===== Flatten choice admitted students ======
@parameterize(
    hss_applications=dict(hss_raw=source("hss_raw")),
)
def flatten_student_options(hss_raw: pd.DataFrame) -> pd.DataFrame:
    enrollment = preprocess_hss_students_enrollment_data(hss_raw)
    return extract_hss_options(enrollment)

# ===== First choice admitted ======
@parameterize(
    hss_first_choice_admissions=dict(hss_student_applications=source("hss_applications")),
)
def filter_first_choice(hss_student_applications: pd.DataFrame) -> pd.DataFrame:
    return filter_admitted_on_first_choice(hss_student_applications)


# ===== Saving HSS Outputs =====
@parameterize(
    save_hss_enrollments=dict(
        df=source("hss_enrollments"),
        dataset_key=value("hss_enrollments"),
    ),
    save_hss_applications=dict(
        df=source("hss_applications"),
        dataset_key=value("hss_applications"),
    ),
    save_hss_marks=dict(
        df=source("hss_marks"),
        dataset_key=value("hss_marks"),
    ),
    save_hss_first_choice_admissions=dict(
        df=source("hss_first_choice_admissions"),
        dataset_key=value("hss_first_choice_admissions"),
    ),
)
def save_hss_data(df: pd.DataFrame, dataset_key: str) -> pd.DataFrame:
    """Generic saver for HSS outputs."""
    logger.info(f"Saving HSS data → {dataset_key}")
    save_data(df, datasets[dataset_key])  # handles directory creation + writing
    return df