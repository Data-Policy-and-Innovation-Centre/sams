from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
import os

# Load environment variables from .env file if it exists
load_dotenv()

# Paths
PROJ_ROOT = Path(__file__).resolve().parents[1]
logger.info(f"PROJ_ROOT path is: {PROJ_ROOT}")

DATA_DIR = PROJ_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
INTERIM_DATA_DIR = DATA_DIR / "interim"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
EXTERNAL_DATA_DIR = DATA_DIR / "external"
LOGS = PROJ_ROOT / "logs"

OUTPUT_DIR = PROJ_ROOT / "output"
FIGURES_DIR = OUTPUT_DIR / "figures"

# Verify that all the directories exist
for path in [DATA_DIR, RAW_DATA_DIR, INTERIM_DATA_DIR, PROCESSED_DATA_DIR, EXTERNAL_DATA_DIR, OUTPUT_DIR, FIGURES_DIR]:
    if not path.exists():
        logger.info(f"Creating directory {path}")
        path.mkdir(parents=True, exist_ok=True)

# Auth
API_AUTH = os.path.join(PROJ_ROOT, "credentials.json")

# Error constants
ERRMAX = 10

# Metadata
STUDENT = {
   "PDIS":{"yearmin":2020,"yearmax":2024},
   "ITI":{"yearmin":2017,"yearmax":2024},
   "Diploma":{"yearmin":2018,"yearmax":2024}
}


INSTITUTE = {
   "PDIS":{"yearmin":2020,"yearmax":2024},
   "ITI":{"yearmin":2017,"yearmax":2024},
   "Diploma":{"yearmin":2018,"yearmax":2024}
}

SOF = {
    "tostring": {1:"Govt", 5:"Pvt"},
    "toint":{"Govt":1,"Pvt":5}
}

NUM_TOTAL_RECORDS = 663679 # Change as needed

try:
    from tqdm import tqdm

    # Remove all handlers
    for handler_id in list(logger._core.handlers.keys()):
        logger.remove(handler_id)

    # Add new logger
    logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)
except ModuleNotFoundError:
    logger.warning("Module tqdm not found")
