from pathlib import Path
import ibis
import duckdb
import os
import time
import psutil
from typing import Any
from loguru import logger
from hamilton.function_modifiers import (
    parameterize,
    source,
    value,
    cache,
)
from sams.config import LOGS, PROJ_ROOT, SAMS_DB, datasets
from sams import utils
from sams.etl.extract import SamsDataDownloader
from sams.etl.orchestrate import SamsDataOrchestrator
from sams.utils import hours_since_creation
from sams.preprocessing.hss_nodes import (
    preprocess_hss_students_enrollment_data,
    extract_hss_options,
    preprocess_students_compartment_marks
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

# ===== Load Raw HSS Student Data =====
@parameterize(
    hss_raw=dict(sams_db=source("sams_db"), module=value("HSS")),
)
@cache(behavior="DISABLE")
def hss_raw(sams_db: Any, module: str) -> ibis.Table:
    """Return Ibis table of HSS students."""
    logger.info(f"Loading raw {module} student data from database")

    table = sams_db.table("students")
    filtered = table.filter(table.module == module)

    count = filtered.count().execute()
    logger.info(f"Loaded {count} records for {module} across all years.")
    return filtered


# Extract enrollments
@parameterize(
    hss_enrollments=dict(df=source("hss_raw")),
)
def preprocess_hss_enrollment(df: ibis.Table) -> ibis.Table:
    return preprocess_hss_students_enrollment_data(df)


# ===== Flatten choice admitted students ======
@parameterize(
    hss_applications=dict(df=source("hss_raw")),
)
def hss_applications(df: ibis.Table) -> ibis.Table:
    return extract_hss_options(df)


# ===== Extract and Preprocess Marks =====
@parameterize(
    hss_marks=dict(df=source("hss_raw")),
)
def extract_preprocess_hss_marks(df: ibis.Table) -> ibis.Table:
    return preprocess_students_compartment_marks(df)


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
)
def save_hss_data(df: Any, dataset_key: str) -> None:
    """
    Generic saver for HSS outputs.
    Saves directly to Parquet (single efficient write).
    """

    output_meta = datasets[dataset_key]
    output_path = str(output_meta["path"])
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    row_count = df.count().execute()
    col_count = len(df.columns)

    if os.path.exists(output_path):
        os.remove(output_path)

    process = psutil.Process()
    start_time = time.time()

    logger.info(f"Saving HSS data as {dataset_key}.pq")
    
    logger.info(f"Shape of the dataset: [rows={row_count}, cols={col_count}]")

    con = df._find_backend().con
    # Enable DuckDB's own progress bar
    con.execute("PRAGMA enable_progress_bar;")

    sql = f"""
    COPY ({df.compile()})
    TO '{output_path}'
    (FORMAT PARQUET, ROW_GROUP_SIZE 500000, COMPRESSION 'zstd');
    """
    con.execute(sql)

    elapsed = time.time() - start_time
    current_mem = process.memory_info().rss / (1024**2)

    logger.info(
    f"HSS data saved → {output_path}, "
    f"mem={current_mem:.1f} MB, time={elapsed:.1f}s"
    )

    return None
