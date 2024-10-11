from sams.etl.orchestrate import SAMSDataOrchestrator
from sams.etl.extract import SamsDataDownloader
from sams.config import PROJ_ROOT, LOGS, RAW_DATA_DIR, SAMS_DB
from pathlib import Path
from loguru import logger
from sams.util import get_existing_modules, hours_since_creation

if not Path(PROJ_ROOT / ".env").exists():
    raise FileNotFoundError(f".env file not found at {PROJ_ROOT}/.env")

downloader = SamsDataDownloader()

if max(hours_since_creation(Path(LOGS / "students_count.csv")), hours_since_creation(Path(LOGS / "institutes_count.csv"))) > 24:
    logger.info("Total records file out of date (if it exists), updating total records...")
    downloader.update_total_records()

orchestrator = SAMSDataOrchestrator(db_url=f"sqlite:///{SAMS_DB}")

# Download student data
excluded_modules = get_existing_modules("students", db_path=f"{SAMS_DB}")
orchestrator.process_data("students",exclude=excluded_modules, bulk_add=False)
