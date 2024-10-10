from sams.etl.orchestrate import SAMSDataOrchestrator
from sams.etl.extract import SamsDataDownloader
from sams.config import PROJ_ROOT, LOGS
from pathlib import Path
from loguru import logger
from sams.util import get_existing_modules

if not Path(PROJ_ROOT / ".env").exists():
    raise FileNotFoundError(f".env file not found at {PROJ_ROOT}/.env")

downloader = SamsDataDownloader()

if not Path(LOGS / "students_count.csv").exists() or not Path(LOGS / "institutes_count.csv").exists():
    logger.info("No total records file found, updating total records...")
    downloader.update_total_records()

orchestrator = SAMSDataOrchestrator()

# Download student data
excluded_modules = get_existing_modules("students")
orchestrator.process_data("students",exclude=excluded_modules)
