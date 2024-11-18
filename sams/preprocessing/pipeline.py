from pathlib import Path
from hamilton.function_modifiers import (
    datasaver,
    parameterize,
    source,
    value,
    load_from,
    save_to,
    cache
)
from hamilton.io import utils
import pandas as pd
import sqlite3
from sams.config import PROJ_ROOT, LOGS, SAMS_DB, datasets
from sams.etl.orchestrate import SamsDataOrchestrator
from sams.etl.extract import SamsDataDownloader
from sams.utils import save_data, hours_since_creation
from sams.preprocessing.nodes import (
    preprocess_iti_students_enrollment_data,
    preprocess_diploma_students_enrollment_data,
    preprocess_students_marks_data,
    preprocess_geocodes,
    preprocess_iti_addresses,
    preprocess_iti_institute_cutoffs,
    preprocess_institute_strength,
    preprocess_distances,
    preprocess_institute_enrollments
    
)
from loguru import logger

# ===== Building raw SAMS data =====
@cache(behavior="DISABLE")
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
@cache(behavior="DISABLE")
def sams_students_raw_df(sams_db: sqlite3.Connection, module: str) -> pd.DataFrame:
    query = f"SELECT * FROM students WHERE module = '{module}' ;"
    df = pd.read_sql_query(query, sams_db)
    return df


@parameterize(
    iti_institutes_raw=dict(sams_db=source("sams_db"), module=value("ITI")),
    diploma_institutes_raw=dict(sams_db=source("sams_db"), module=value("Diploma")),
)
@cache(behavior="DISABLE")
def sams_institutes_raw_df(sams_db: sqlite3.Connection, module: str) -> pd.DataFrame:
    query = f"SELECT * FROM institutes WHERE module = '{module}';"
    df = pd.read_sql_query(query, sams_db)
    return df

@cache(behavior="DISABLE")
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
@cache(behavior="default")
def enrollment_df(sams_students_raw_df: pd.DataFrame, module: str) -> pd.DataFrame:
    logger.info(f"Preprocessing {module} students enrollment data...")
    if module == "ITI":
        return preprocess_iti_students_enrollment_data(sams_students_raw_df)
    elif module == "Diploma":
        return preprocess_diploma_students_enrollment_data(sams_students_raw_df)
    else:
        NotImplemented

@load_from.csv(path=datasets["iti_addresses"]["path"])
@cache(behavior="default")
def iti_addresses_df(iti_address_raw_df: pd.DataFrame) -> pd.DataFrame:
    return preprocess_iti_addresses(iti_address_raw_df)


@cache(behavior="default")
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
        geocoded_iti_institutes= dict(
            sams_institutes_raw_df=source("iti_institutes_raw"),
            geocodes_df=source("geocodes_df"),
            iti_addresses_df=source("iti_addresses_df")
        ),
)
@cache(behavior="default")
def geocoded_institutes_df(sams_institutes_raw_df: pd.DataFrame, geocodes_df: pd.DataFrame, iti_addresses_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Adding geocodes to institutes data...")
    iti_addresses_df = pd.merge(
        iti_addresses_df, geocodes_df, how="left", on="address"
    )
    geocoded_institutes_df = sams_institutes_raw_df[["ncvtmis_code","sams_code"]].drop_duplicates()
    geocoded_institutes_df = pd.merge(
        geocoded_institutes_df, iti_addresses_df, how="left", on="ncvtmis_code"
    )
    geocoded_institutes_df = geocoded_institutes_df[["sams_code", "latitude", "longitude"]]
    return geocoded_institutes_df

@parameterize(
    geocoded_iti_enrollment=dict(
        enrollment_df=source("iti_enrollment"),
        geocodes_df=source("geocodes_df"),
        geocoded_institutes_df=source("geocoded_iti_institutes"),
        module=value("ITI")
    ),
    geocoded_diploma_enrollment=dict(
        enrollment_df=source("diploma_enrollment"),
        geocodes_df=source("geocodes_df"),
        geocoded_institutes_df=value(None),
        module=value("Diploma"),
    ),
)
@cache(behavior="default")
def geocoded_enrollment_df(
    enrollment_df: pd.DataFrame, geocodes_df: pd.DataFrame, geocoded_institutes_df: pd.DataFrame, module: str
) -> pd.DataFrame:
    logger.info(f"Adding geocodes to {module} enrollment data...")
    geocoded_enrollment_df = pd.merge(
        enrollment_df, geocodes_df, how="left", left_on="pin_code", right_on="address"
    )
    geocoded_enrollment_df.drop("address_y", axis=1, inplace=True)
    geocoded_enrollment_df.rename(
        columns={
            "latitude": "student_lat",
            "longitude": "student_long",
            "address_x": "address",
        },
        inplace=True,
    )
    if module == "ITI":
        geocoded_enrollment_df = pd.merge(
            geocoded_enrollment_df, geocoded_institutes_df, how="left", on="sams_code"
        )
        geocoded_enrollment_df.rename(
            columns={
                "latitude": "institute_lat",
                "longitude": "institute_long",
            },
            inplace=True,
        )
        geocoded_enrollment_df = preprocess_distances(geocoded_enrollment_df)
    else:
        NotImplemented

    return geocoded_enrollment_df



@parameterize(
    iti_marks=dict(enrollment_df=source("iti_enrollment")),
    diploma_marks=dict(enrollment_df=source("diploma_enrollment")),
)
@cache(behavior="default")
def marks_df(enrollment_df: pd.DataFrame) -> pd.DataFrame:
    logger.info(
        f"Preprocessing {enrollment_df.module.values[0]} students marks data..."
    )
    return preprocess_students_marks_data(enrollment_df)

@parameterize(
    iti_institutes_strength = dict(
        sams_institutes_raw_df=source("iti_institutes_raw"),
        geocoded_institutes_df=source("geocoded_iti_institutes"),
    ),
    diploma_institutes_strength = dict(
        sams_institutes_raw_df=source("diploma_institutes_raw"),
        geocoded_institutes_df=value(None),
    ),
)
@cache(behavior="default")
def institutes_strength_df(sams_institutes_raw_df: pd.DataFrame, geocoded_institutes_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Preprocessing institute strength data...")
    institutes_strength =  preprocess_institute_strength(sams_institutes_raw_df)
    if geocoded_institutes_df is not None:
        institutes_strength = pd.merge(institutes_strength, geocoded_institutes_df, how="left", on="sams_code")
    return institutes_strength


@parameterize(
    iti_institutes_cutoffs = dict(
        sams_institutes_raw_df=source("iti_institutes_raw"),
        module=value("ITI")
    )
)
@cache(behavior="default")
def institutes_cutoff_df(sams_institutes_raw_df: pd.DataFrame, module: str) -> pd.DataFrame:
    logger.info(f"Preprocessing {module} institute cutoff data...")
    return preprocess_iti_institute_cutoffs(sams_institutes_raw_df)

@parameterize(
    iti_institutes_enrollments = dict(
        sams_institutes_raw_df=source("iti_institutes_raw"),
    ),
    diploma_institutes_enrollments = dict(
        sams_institutes_raw_df=source("diploma_institutes_raw"),
    ),
)
@cache(behavior="default")
def institutes_enrollments_df(sams_institutes_raw_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Preprocessing institute enrollments data...")
    return preprocess_institute_enrollments(sams_institutes_raw_df)

def _refactor_social_category(category: str, orphan: bool, gc: bool, ph: bool, es: bool, ews: bool) -> str:
    if orphan:
        return "ORPHAN"
    if gc:
        return "GC"
    if ph != "No":
        return "PWD"
    if es:
        return "ES"
    if ews:
        return "EWS"

    if category == "General" or category == "OBC/SEBC":
        return "UR"
    elif "SC" in category:
        return "SC"
    elif "ST" in category:
        return "ST"
     
@save_to.parquet(path=value(datasets["iti_marks_and_cutoffs"]["path"]))
@cache(behavior="default")
def iti_marks_and_cutoffs(geocoded_iti_enrollment: pd.DataFrame, iti_marks: pd.DataFrame, iti_institutes_cutoffs: pd.DataFrame) -> pd.DataFrame:
    logger.info("Generating ITI marks and cutoffs joined frame for marks/cutoffs analysis...")
    academics_demographics = geocoded_iti_enrollment[["aadhar_no","reported_branch_or_trade","phase","academic_year","sams_code", "gender","social_category","gc", "ph",
                                                       "es","orphan", "ews", "reported_institute", "institute_district"]]
    academics_demographics["local"] = (academics_demographics["reported_institute"] == academics_demographics["institute_district"])
    
    marks = pd.merge(
        iti_marks, academics_demographics, on=["aadhar_no", "academic_year"]
    )
    marks["social_category"] = marks.apply(lambda row: _refactor_social_category(row["social_category"],row["orphan"],row["gc"], row["ph"],row["es"],row["ews"]), axis=1)
    marks.drop(columns=["orphan", "gc", "ph", "es", "ews", "reported_institute", "institute_district"], inplace=True)
    marks["phase"] = marks["phase"].apply(lambda x: int(x) if x is not None else -1)
    marks_cutoffs = pd.merge(
        marks, iti_institutes_cutoffs, left_on=["sams_code", "academic_year", "reported_branch_or_trade", "social_category","gender", "phase", "local", "exam_name"], 
        right_on=["sams_code", "academic_year","trade", "social_category","gender", "selection_stage", "local","qual"],
        how="left"
    )
    marks_cutoffs.drop(columns=["compartmental_status","compartmental_fail_mark", 
                                "highest_qualification_board_exam_name","phase", "selection_stage",
                                "reported_branch_or_trade", "applicant_type", "exam_name"], inplace=True)
    marks_cutoffs.drop_duplicates(subset=["aadhar_no", "academic_year", "sams_code", "trade", "social_category","gender", "local", "qual"], inplace=True)
    marks_cutoffs = marks_cutoffs[marks_cutoffs["percentage"] > marks_cutoffs["cutoff"]]
    #save_data(marks_cutoffs, datasets["iti_marks_and_cutoffs"])
    return marks_cutoffs

@save_to.parquet(path=value(datasets["iti_vacancies"]["path"]))
@cache(behavior="default")
def iti_vacancies(geocoded_iti_enrollment: pd.DataFrame, iti_institutes_strength: pd.DataFrame) -> pd.DataFrame:
    logger.info("Generating ITI vacancies data...")
    iti_enrollments_agg = geocoded_iti_enrollment.groupby(["sams_code","academic_year","reported_branch_or_trade"], as_index=False)["aadhar_no"].count()
    iti_strength = iti_institutes_strength[iti_institutes_strength["category"] == "Total"]
    iti_enrollments_strength = pd.merge(
        iti_enrollments_agg, iti_strength, left_on=["sams_code","academic_year","reported_branch_or_trade"], right_on=["sams_code","academic_year","trade"],
        how="inner", indicator=False
    )
    iti_enrollments_strength.rename(columns={"aadhar_no": "enrollment"}, inplace=True)
    iti_enrollments_strength["vacancies"] = iti_enrollments_strength["strength"] - iti_enrollments_strength["enrollment"]
    iti_enrollments_strength["vacancy_ratio"] = iti_enrollments_strength["vacancies"] / iti_enrollments_strength["strength"]
    iti_enrollments_strength.drop(columns=["branch","reported_branch_or_trade"], inplace=True)
    iti_enrollments_strength[["enrollment","strength","vacancies"]] = iti_enrollments_strength[["enrollment","strength","vacancies"]].astype(int)
    return iti_enrollments_strength

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
        institutes_cutoff_df=source("iti_institutes_cutoffs"),
        institutes_enrollment_df=source("iti_institutes_enrollments"),
        module=value("ITI"),
    ),
    save_interim_diploma_institutes=dict(
        institutes_strength_df=source("diploma_institutes_strength"),
        institutes_cutoff_df=value(None),
        institutes_enrollment_df=source("diploma_institutes_enrollments"),
        module=value("Diploma"),
    ),
        
)
def save_interim_institutes_data(institutes_strength_df: pd.DataFrame, institutes_cutoff_df: pd.DataFrame, institutes_enrollment_df: pd.DataFrame, module: str) -> dict:
    logger.info(f"Saving interim institutes data for {module} module...")
    module = module.lower()
    save_data(institutes_strength_df, datasets[f"{module}_institutes_strength"])
    save_data(institutes_enrollment_df, datasets[f"{module}_institutes_enrollments"])
    
    if module == "iti":
        save_data(institutes_cutoff_df, datasets[f"{module}_institutes_cutoffs"])
        metadata = {
        f"{module}_institutes_strength": utils.get_file_and_dataframe_metadata(
            datasets[f"{module}_institutes_strength"]["path"], institutes_strength_df
        ),
        f"{module}_institutes_cutoff": utils.get_file_and_dataframe_metadata(
            datasets[f"{module}_institutes_cutoffs"]["path"], institutes_cutoff_df
        ),
        f"{module}_institutes_enrollments": utils.get_file_and_dataframe_metadata(
            datasets[f"{module}_institutes_enrollments"]["path"], institutes_enrollment_df
        ),
    }
    else:
        metadata = {
        f"{module}_institutes_strength": utils.get_file_and_dataframe_metadata(
            datasets[f"{module}_institutes_strength"]["path"], institutes_strength_df
        ),
        f"{module}_institutes_enrollments": utils.get_file_and_dataframe_metadata(
            datasets[f"{module}_institutes_enrollments"]["path"], institutes_enrollment_df
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
