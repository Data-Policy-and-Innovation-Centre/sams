from pathlib import Path
from dotenv import load_dotenv
from loguru import logger
import os
import yaml

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
MISSING_VALUES = LOGS / "missing_values"
OUTPUT_DIR = PROJ_ROOT / "output"
FIGURES_DIR = OUTPUT_DIR / "figures"
CONFIG = PROJ_ROOT / "config" 

# Verify that all the directories exist
for path in [
    DATA_DIR,
    RAW_DATA_DIR,
    INTERIM_DATA_DIR,
    PROCESSED_DATA_DIR,
    EXTERNAL_DATA_DIR,
    OUTPUT_DIR,
    FIGURES_DIR,
    LOGS,
    MISSING_VALUES,
]:
    if not path.exists():
        logger.info(f"Creating directory {path}")
        path.mkdir(parents=True, exist_ok=True)

# Data catalog
with open(CONFIG / "datasets.yaml") as f:
    datasets = yaml.safe_load(f)
    datasets = datasets["datasets"]

SAMS_DB = PROJ_ROOT / Path(datasets["sams"]["path"])


def get_path(name: str) -> Path:
    return PROJ_ROOT / Path(datasets[name]["path"])


# Auth
USERNAME = os.getenv("SAMSAPI_USERNAME")
PASSWORD = os.getenv("SAMSAPI_PASSWORD")

# Error constants
ERRMAX = 3

# Metadata
STUDENT = {
    "PDIS": {"yearmin": 2020, "yearmax": 2024},
    "ITI": {"yearmin": 2017, "yearmax": 2024},
    "Diploma": {"yearmin": 2018, "yearmax": 2024},
}


INSTITUTE = {
    "PDIS": {"yearmin": 2020, "yearmax": 2024},
    "ITI": {"yearmin": 2017, "yearmax": 2024},
    "Diploma": {"yearmin": 2018, "yearmax": 2024},
}


try:
    from tqdm import tqdm

    # Remove all handlers
    for handler_id in list(logger._core.handlers.keys()):
        logger.remove(handler_id)

    # Add new logger
    logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)
except ModuleNotFoundError:
    logger.warning("Module tqdm not found")
