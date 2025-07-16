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
from sams.config import datasets,LOGS,PROJ_ROOT,SAMS_DB
from sams.etl.extract import SamsDataDownloader
from sams.etl.orchestrate import SamsDataOrchestrator
from sams.utils import save_data, hours_since_creation, load_data
from sams.preprocessing.hss_nodes import (
    preprocess_hss_students_enrollment_data,
    extract_hss_compartments,
    extract_hss_options,
    extract_hss_admitted_option,
    compute_local_flag,
    merge_institute_geocodes,
    preprocess_distances,
    preprocess_students_compartment_marks,
    admitted_students_only
    )

# Connect to or build the database
@cache(behavior="DISABLE")
def sams_db(build: bool = False) -> sqlite3.Connection:
    if Path(SAMS_DB).exists() and not build:
        logger.info(f"Using existing database at {SAMS_DB}")
        return sqlite3.connect(SAMS_DB)

    if build:
        logger.info("Building SAMS database...")
        downloader = SamsDataDownloader()
        orchestrator = SamsDataOrchestrator(db_url=f"sqlite:///{SAMS_DB}")
        orchestrator.process_data("institutes", exclude=True, bulk_add=True)
        orchestrator.process_data("students", exclude=True, bulk_add=True)
        return sqlite3.connect(SAMS_DB)

    raise FileNotFoundError(f"No database found at {SAMS_DB}")

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

# ===== Load Raw HSS Data from SAMS =====
@parameterize(
    hss_raw=dict(sams_db=source("sams_db"), module=value("HSS")),
)
@cache(behavior="DISABLE")
def sams_students_raw_df(sams_db: sqlite3.Connection, module: str) -> pd.DataFrame:
    logger.info(f"Loading {module} students raw data from database")
    query = f"SELECT * FROM students WHERE module = '{module}';"
    return pd.read_sql_query(query, sams_db)

# ===== Preprocess Raw HSS Data =====
@parameterize(
    hss_enrollment=dict(df=source("hss_raw")),
)
def preprocess_enrollment_data(df: pd.DataFrame) -> pd.DataFrame:
    return preprocess_hss_students_enrollment_data(df)

# ===== Extract Marks from Enrollment =====
@parameterize(
    hss_marks=dict(df=source("hss_enrollment")),
)
def extract_hss_compartments(df: pd.DataFrame) -> pd.DataFrame:
    return extract_hss_compartments(df)

# ===== Clean Compartment Marks =====
@parameterize(
    hss_marks_clean=dict(df=source("hss_marks")),
)
def preprocess_hss_compartment_data(df: pd.DataFrame) -> pd.DataFrame:
    return preprocess_students_compartment_marks(df)

# ===== Extract Option Details =====
@parameterize(
    hss_options_extracted=dict(df=source("hss_enrollment")),
)
def extract_hss_options(df: pd.DataFrame) -> pd.DataFrame:
    return extract_hss_options(df["hss_option_details"])

# ===== Extract Admitted Option from Options =====
@parameterize(
    hss_admitted =dict(df=source("hss_enrollment")),
)
def extract_best_hss_option(df: pd.DataFrame) -> pd.DataFrame:
    return extract_hss_admitted_option(df["hss_option_details"])

# ===== Filter Admitted Students Only =====
@parameterize(
    hss_admitted_first_option=dict(hss_admitted_option=source("hss_admitted_option_all")),
)
def filter_admitted_option(hss_admitted_option: pd.DataFrame) -> pd.DataFrame:
    from sams.preprocessing.hss_nodes import admitted_students_only
    return admitted_students_only(hss_admitted_option)

# ===== Enrich Enrollment with Admitted Option =====
@parameterize(
   hss_enrollment_with_admitted_option =dict(
        enrollment_df=source("hss_enrollment"),
        admitted_df=source("hss_admitted_option"),
    )
)
def enrich_with_admitted_info(enrollment_df: pd.DataFrame, admitted_df: pd.DataFrame) -> pd.DataFrame:
    return pd.merge(
        enrollment_df,
        admitted_df,
        on="barcode",
        how="left",
        suffixes=("", "_admitted")
    )

# ===== Compute Local Flag =====
@parameterize(
    hss_enrollment_with_local=dict(df=source("hss_enrollment_with_admitted_option")),
)
def compute_local_flag_node(df: pd.DataFrame) -> pd.DataFrame:
    return compute_local_flag(df)

# ===== Add Institute Geocodes =====
@parameterize(
    hss_with_institute_coords=dict(
        df=source("hss_enrollment_with_local"),
        geocode_df=source("institute_geocodes_df"),
    )
)
def merge_geocodes(df: pd.DataFrame, geocode_df: pd.DataFrame) -> pd.DataFrame:
    return merge_institute_geocodes(df, geocode_df)

# ===== Compute Distance to Institute =====
@parameterize(
    hss_with_distance=dict(df=source("hss_with_institute_coords")),
)
def compute_distances(df: pd.DataFrame) -> pd.DataFrame:
    return preprocess_distances(df)


# ===== save to parquet =====
@save_to.parquet(path=value(datasets["hss_geocoded_with_marks"]["path"]))
def hss_geocoded_with_marks(hss_with_distance: pd.DataFrame, hss_marks_clean: pd.DataFrame) -> pd.DataFrame:
    logger.info("Saving final HSS geocoded dataset with marks...")

    student_fields = [
        "barcode", "academic_year", "dob", "gender", "social_category",
        "district", "institute_district", "SAMSCode", "OptionNo",
        "latitude", "longitude", "institute_lat", "institute_long", "distance", "local"
    ]

    demographics = hss_with_distance[student_fields].drop_duplicates(subset=["barcode", "academic_year"])
    marks = pd.merge(hss_marks_clean, demographics, on=["barcode", "academic_year"], how="left")
    marks = marks.dropna(subset=["SAMSCode", "institute_lat", "institute_long"])
    return marks

# ===== SAVE: Non-Geocoded HSS Students =====
@datasaver()
@parameterize(
    save_nongeocoded_hss_students=dict(
        enrollment_df=source("hss_enrollment"),
        marks_df=source("hss_marks_clean"),  # or hss_compartments_long
        module=value("HSS"),
    )
)
def save_nongeocoded_student_data(
    enrollment_df: pd.DataFrame, marks_df: pd.DataFrame, module: str
) -> dict:
    logger.info(f"Saving non-geocoded student data for {module} module...")
    module = module.lower()

    save_data(enrollment_df, datasets[f"{module}_enrollments"])
    save_data(marks_df, datasets[f"{module}_marks"])

    metadata = {
        f"{module}_enrollments": utils.get_file_and_dataframe_metadata(
            datasets[f"{module}_enrollments"]["path"], enrollment_df
        ),
        f"{module}_marks": utils.get_file_and_dataframe_metadata(
            datasets[f"{module}_marks"]["path"], marks_df
        ),
    }

    return metadata


# ===== SAVE: Geocoded HSS Students =====
@datasaver()
@parameterize(
    save_geocoded_hss_students=dict(
        enriched_df=source("hss_with_distance"),
        module=value("HSS"),
    )
)
def save_geocoded_hss_students(enriched_df: pd.DataFrame, module: str) -> dict:
    logger.info(f"Saving geocoded HSS enriched dataset...")

    module = module.lower()
    save_data(enriched_df, datasets[f"{module}_geocoded"])

    metadata = {
        f"{module}_geocoded": utils.get_file_and_dataframe_metadata(
            datasets[f"{module}_geocoded"]["path"], enriched_df
        )
    }
    return metadata
