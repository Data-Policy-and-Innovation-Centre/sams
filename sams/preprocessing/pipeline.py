from pathlib import Path
from hamilton.function_modifiers import (
    datasaver,
    parameterize,
    source,
    value,
    load_from,
    save_to,
)
from hamilton.io import utils
import pandas as pd
import sqlite3
from sams.config import PROJ_ROOT, LOGS, SAMS_DB, datasets
from sams.etl.orchestrate import SamsDataOrchestrator
from sams.etl.extract import SamsDataDownloader
from sams.util import save_data, hours_since_creation
from sams.preprocessing.nodes import (
    preprocess_iti_students_enrollment_data,
    preprocess_diploma_students_enrollment_data,
    preprocess_students_marks_data,
    preprocess_geocodes,
    preprocess_iti_addresses,
    preprocess_iti_institute_cutoffs,
    preprocess_institute_strength
    
)
from loguru import logger

# ===== Building raw SAMS data =====
def sams_db(build: bool = True) -> sqlite3.Connection:
    if Path(SAMS_DB).exists() and not build:
        logger.info(f"Using existing database at {SAMS_DB}")
        return sqlite3.connect(SAMS_DB)

    if build:
        logger.info(f"Building database at {SAMS_DB} from SAMS API ")

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
            logger.info(
                "Total records file out of date (if it exists), updating total records..."
            )
            downloader.update_total_records()

        orchestrator = SamsDataOrchestrator(db_url=f"sqlite:///{SAMS_DB}")

        # Download data from SAMS API
        orchestrator.process_data("institutes", exclude=True, bulk_add=True)
        orchestrator.process_data("students", exclude=True, bulk_add=True)

        return sqlite3.connect(SAMS_DB)
    else:
        raise FileNotFoundError(f"Database not found at {SAMS_DB}")

# ===== Loading data =====
@parameterize(
    iti_raw=dict(sams_db=source("sams_db"), module=value("ITI")),
    diploma_raw=dict(sams_db=source("sams_db"), module=value("Diploma")),
)
def sams_students_raw_df(sams_db: sqlite3.Connection, module: str) -> pd.DataFrame:
    query = f"SELECT * FROM students WHERE module = '{module}';"
    df = pd.read_sql_query(query, sams_db)
    return df


@parameterize(
    iti_institutes_raw=dict(sams_db=source("sams_db"), module=value("ITI")),
    diploma_institutes_raw=dict(sams_db=source("sams_db"), module=value("Diploma")),
)
def sams_institutes_raw_df(sams_db: sqlite3.Connection, module: str) -> pd.DataFrame:
    query = f"SELECT * FROM institutes WHERE module = '{module}';"
    df = pd.read_sql_query(query, sams_db)
    return df


def sams_address_raw_df(sams_db: sqlite3.Connection) -> pd.DataFrame:
    query = "SELECT pin_code FROM students;"
    df = pd.DataFrame(pd.read_sql_query(query, sams_db))
    return df


# ===== Preprocessing =====
@parameterize(
    iti_enrollment=dict(sams_students_raw_df=source("iti_raw"), module=value("ITI")),
    diploma_enrollment=dict(
        sams_students_raw_df=source("diploma_raw"), module=value("Diploma")
    ),
)
def enrollment_df(sams_students_raw_df: pd.DataFrame, module: str) -> pd.DataFrame:
    logger.info(f"Preprocessing {module} students enrollment data...")
    if module == "ITI":
        return preprocess_iti_students_enrollment_data(sams_students_raw_df)
    elif module == "Diploma":
        return preprocess_diploma_students_enrollment_data(sams_students_raw_df)
    else:
        NotImplemented

@load_from.csv(path=datasets["iti_addresses"]["path"])
def iti_addresses_df(iti_address_raw_df: pd.DataFrame) -> pd.DataFrame:
    return preprocess_iti_addresses(iti_address_raw_df)


def geocodes_df(
    sams_address_raw_df: pd.DataFrame, iti_addresses_df: pd.DataFrame, google_maps: bool
) -> pd.DataFrame:
    logger.info("Preprocessing geocodes...")
    return preprocess_geocodes(
        [sams_address_raw_df, iti_addresses_df],
        address_col=["pin_code", "address"],
        google_maps=google_maps,
    )


@parameterize(
    geocoded_iti_enrollment=dict(
        enrollment_df=source("iti_enrollment"),
        geocodes_df=source("geocodes_df"),
        module=value("ITI"),
    ),
    geocoded_diploma_enrollment=dict(
        enrollment_df=source("diploma_enrollment"),
        geocodes_df=source("geocodes_df"),
        module=value("Diploma"),
    ),
)
def geocoded_enrollment_df(
    enrollment_df: pd.DataFrame, geocodes_df: pd.DataFrame, module: str
) -> pd.DataFrame:
    logger.info(f"Adding geocodes to {module} enrollment data...")
    merged = pd.merge(
        enrollment_df, geocodes_df, how="left", left_on="pin_code", right_on="address"
    )
    merged.drop("address_y", axis=1, inplace=True)
    merged.rename(
        columns={
            "latitude": "pin_lat",
            "longitude": "pin_long",
            "address_x": "address",
        },
        inplace=True,
    )
    return merged


@parameterize(
    iti_marks=dict(enrollment_df=source("iti_enrollment")),
    diploma_marks=dict(enrollment_df=source("diploma_enrollment")),
)
def marks_df(enrollment_df: pd.DataFrame) -> pd.DataFrame:
    logger.info(
        f"Preprocessing {enrollment_df.module.values[0]} students marks data..."
    )
    return preprocess_students_marks_data(enrollment_df)

@parameterize(
    iti_institutes_strength = dict(
        sams_institutes_raw_df=source("iti_institutes_raw")
    )
)
def institutes_strength_df(sams_institutes_raw_df: pd.DataFrame) -> pd.DataFrame:
    return preprocess_institute_strength(sams_institutes_raw_df)

@parameterize(
    iti_institutes_cutoff = dict(
        sams_institutes_raw_df=source("iti_institutes_raw")
    )
)
def institutes_cutoff_df(sams_institutes_raw_df: pd.DataFrame) -> pd.DataFrame:
    return preprocess_iti_institute_cutoffs(sams_institutes_raw_df)


# ===== Saving data =====
@datasaver()
@parameterize(
    save_interim_iti_students=dict(
        enrollment_df=source("geocoded_iti_enrollment"),
        marks_df=source("iti_marks"),
        module=value("ITI"),
    ),
    save_interim_diploma_students=dict(
        enrollment_df=source("geocoded_diploma_enrollment"),
        marks_df=source("diploma_marks"),
        module=value("Diploma"),
    ),
)
def save_interim_student_data(
    enrollment_df: pd.DataFrame, marks_df: pd.DataFrame, module: str
) -> dict:
    logger.info(f"Saving interim student data for {module} module...")
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

@datasaver()
@parameterize(
    save_interim_iti_institutes=dict(
        institutes_strength_df=source("iti_institutes_strength"),
        institutes_cutoff_df=source("iti_institutes_cutoff"),
        module=value("ITI"),
    )
        
)
def save_interim_institutes_data(institutes_strength_df: pd.DataFrame, institutes_cutoff_df: pd.DataFrame, module: str) -> dict:
    logger.info(f"Saving interim institutes data for {module} module...")
    module = module.lower()
    save_data(institutes_strength_df, datasets[f"{module}_institutes_strength"])
    save_data(institutes_cutoff_df, datasets[f"{module}_institutes_cutoffs"])

    metadata = {
        f"{module}_institutes_strength": utils.get_file_and_dataframe_metadata(
            datasets[f"{module}_institutes_strength"]["path"], institutes_strength_df
        ),
        f"{module}_institutes_cutoff": utils.get_file_and_dataframe_metadata(
            datasets[f"{module}_institutes_cutoffs"]["path"], institutes_cutoff_df
        ),
    }

    return metadata


@datasaver()
def save_geocodes(geocodes_df: pd.DataFrame) -> dict:
    logger.info("Saving geocodes...")
    save_data(geocodes_df, datasets["geocodes"])

    return utils.get_file_and_dataframe_metadata(
        datasets["geocodes"]["path"], geocodes_df
    )
