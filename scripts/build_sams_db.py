from sams.etl.orchestrate import SamsDataOrchestrator
from sams.etl.extract import SamsDataDownloader
from sams.config import PROJ_ROOT, LOGS, SAMS_DB
from pathlib import Path
from loguru import logger
from sams.utils import hours_since_creation
import requests

if not Path(PROJ_ROOT / ".env").exists():
    raise FileNotFoundError(f".env file not found at {PROJ_ROOT}/.env")

downloader = SamsDataDownloader()

try:
    if (
        max(
            hours_since_creation(Path(LOGS / "students_count.csv")),
            hours_since_creation(Path(LOGS / "institutes_count.csv")),
        )
        > 24
    ):
        logger.info("Total records file out of date (if it exists), updating total records...")
        try:
            downloader.update_total_records()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Skipping total records update due to API error: {e}")

except FileNotFoundError:
    logger.warning("Total record file(s) missing — possibly deleted or first run. Trying to update them now.")
    try:
        downloader.update_total_records()
    except requests.exceptions.RequestException as e:
        logger.warning("Could not update total records — API not accesible.")
        logger.warning(f"Skipping total records update due to API error: {e}")

orchestrator = SamsDataOrchestrator(db_url=f"sqlite:///{SAMS_DB}")

# Check if student count file exists
if Path(LOGS / "students_count.csv").exists():
    orchestrator.process_data("students", exclude=True, bulk_add=True)
else:
    logger.warning("Skipping student data load: 'students_count.csv' not found. Run extract.py to generate it, or check if the API is currently unavailable.")

# Step 4: Check if institute count file exists
if Path(LOGS / "institutes_count.csv").exists():
    orchestrator.process_data("institutes", exclude=True, bulk_add=True)
else:
    logger.warning("Skipping institute data load: 'institutes_count.csv' not found. Run extract.py to generate it, or check if the API is currently unavailable.")


# Download student data
# orchestrator.process_data("students", exclude=True, bulk_add=True)
# orchestrator.process_data("institutes", exclude=True, bulk_add=True)