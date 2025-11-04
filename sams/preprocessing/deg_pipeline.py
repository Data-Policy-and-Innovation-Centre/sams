from pathlib import Path
import ibis
import duckdb
import os
import psutil
import time
from tqdm import tqdm
from loguru import logger
from typing import Any
from hamilton.function_modifiers import (
    parameterize,
    source,
    value,
    cache,
)
from sams.config import LOGS, PROJ_ROOT, SAMS_DB, datasets
from sams.etl.extract import SamsDataDownloader
from sams.etl.orchestrate import SamsDataOrchestrator
from sams.utils import hours_since_creation
from sams.preprocessing.deg_nodes import (
    preprocess_deg_students_enrollment_data,
    preprocess_deg_options_details,
    preprocess_deg_compartments
)


# Build or Load SAMS Database 
@cache(behavior="DISABLE")
def sams_db(build: bool = True) -> Any:
    """Return an Ibis connection to the SAMS SQLite database"""
    
    if Path(SAMS_DB).exists() and not build:
        logger.info(f"Using existing database at {SAMS_DB}")
        return ibis.duckdb.connect(str(SAMS_DB))

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

        return ibis.duckdb.connect(SAMS_DB)

    raise FileNotFoundError(f"Database not found at {SAMS_DB}")

# Load Raw DEG Student Data
@parameterize(
    deg_raw=dict(sams_db=source("sams_db"), module=value("DEG")),
)
@cache(behavior="DISABLE")
def deg_raw(sams_db: Any, module: str) -> Any:
    """Return an Ibis table filtered on module."""
    logger.info(f"Loading raw {module} student data from database")

    table = sams_db.table("students")
    filtered = table.filter(table.module == module)
    # log row count for overall dataset
    count = filtered.count().execute()
    logger.info(f"Loaded {count} records for {module} across all years")

    return filtered


# Preprocess DEG Enrollment Data 
@parameterize(deg_enrollments=dict(df=source("deg_raw")))
def preprocess_deg_enrollment(df: Any) -> Any:
    """
    Clean and prepare raw DEG enrollment data.
    Returns structured enrollment records.
    """ 
    return preprocess_deg_students_enrollment_data(df)

# Preprocess DEG application data
@parameterize(deg_applications=dict(students_table=source("deg_raw")))
def preprocess_deg_applications(students_table: ibis.Table) -> ibis.Table:   
    """
    Flatten and clean DEG application data.
    Returns one row per application option per student.
    """ 
    return preprocess_deg_options_details(students_table)

# Preprocess DEG marks
@parameterize(deg_marks=dict(students_table=source("deg_raw")))
def preprocess_deg_marks(students_table: ibis.Table) -> ibis.Table:
    """
    Clean and structure DEG marks and compartment information.
    Returns one row per compartment per student.
    """
    return preprocess_deg_compartments(students_table)


# ===== Saving DEG Outputs =====
@parameterize(
    save_deg_enrollments=dict(
        df=source("deg_enrollments"),
        dataset_key=value("deg_enrollments"),
    ),
    save_deg_applications=dict(
        df=source("deg_applications"),
        dataset_key=value("deg_applications"),
    ),
    save_deg_marks=dict(
        df=source("deg_marks"),
        dataset_key=value("deg_marks"),
    ),
)
def save_deg_data(df: ibis.Table, dataset_key: str) -> None:
    """
    Generic saver for DEG outputs.
    Saves data directly to Parquet with DuckDB COPY (single pass, fastest runtime).
    Shows progress bar during the operation.
    """
    output_meta = datasets[dataset_key]
    output_path = str(output_meta["path"])
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Remove old file if exists
    if os.path.exists(output_path):
        os.remove(output_path)

    # Get shape before saving
    row_count = df.count().execute()
    col_count = len(df.columns)

    process = psutil.Process()
    start_time = time.time()

    logger.info(f"Saving DEG data as {dataset_key}.pq")
    logger.info(f"Shape of the dataset: [rows={row_count:,}, cols={col_count}]")

    con = df._find_backend().con
    con.execute("PRAGMA enable_progress_bar;")  # D

    # Wrap COPY inside tqdm so the bar appears once (0 → 100%)
    with tqdm(total=1, desc=f"Saving {dataset_key}.pq", unit="task") as pbar:
        sql = f"""
        COPY ({df.compile()})
        TO '{output_path}'
        (FORMAT PARQUET, ROW_GROUP_SIZE 500000, COMPRESSION 'zstd');
        """
        con.execute(sql)
        pbar.update(1)

    elapsed = time.time() - start_time
    current_mem = process.memory_info().rss / (1024**2)

    logger.info(
        f"DEG data saved → {dataset_key}.pq "
        f"[rows={row_count:,}, cols={col_count}, "
        f"time={elapsed:.1f}s]"
    )
    return None
