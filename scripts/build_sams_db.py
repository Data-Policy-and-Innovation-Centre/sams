from sams.etl.orchestrate import SamsDataOrchestrator
from sams.etl.extract import SamsDataDownloader
from sams.config import PROJ_ROOT, LOGS, SAMS_DB
from pathlib import Path
from loguru import logger
from sams.util import hours_since_creation

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

# Download student data
orchestrator.process_data("institutes", exclude=True, bulk_add=True)
orchestrator.process_data("students", exclude=True, bulk_add=True)
