from pathlib import Path
from dotenv import load_dotenv
from loguru import logger
import os
import yaml
import pickle
from geopy.geocoders import Nominatim, GoogleV3
from geopy.extra.rate_limiter import RateLimiter

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
SCTEVT_DIR = EXTERNAL_DATA_DIR / "sctevt"
LOGS = PROJ_ROOT / "logs"
MISSING_VALUES = LOGS / "missing_values"
OUTPUT_DIR = PROJ_ROOT / "output"
FIGURES_DIR = OUTPUT_DIR / "figures"
TABLES_DIR = OUTPUT_DIR / "tables"
CONFIG = PROJ_ROOT / "config"
CACHE = PROJ_ROOT / "cache"

# Verify that all the directories exist
for path in [
    DATA_DIR,
    RAW_DATA_DIR,
    INTERIM_DATA_DIR,
    PROCESSED_DATA_DIR,
    EXTERNAL_DATA_DIR,
    SCTEVT_DIR,
    OUTPUT_DIR,
    FIGURES_DIR,
    TABLES_DIR,
    LOGS,
    MISSING_VALUES,
    CACHE,
]:
    if not path.exists():
        logger.info(f"Creating directory {path}")
        path.mkdir(parents=True, exist_ok=True)

# Data catalog
with open(CONFIG / "catalog.yaml") as f:
    catalog = yaml.safe_load(f)
    datasets = catalog["datasets"]
    exhibits = catalog["exhibits"]

SAMS_DB = PROJ_ROOT / Path(datasets["sams"]["path"])

for name in datasets:
    datasets[name]["path"] = PROJ_ROOT / Path(datasets[name]["path"])

for name in exhibits:
    if "path" in exhibits[name].keys():
        exhibits[name]["path"] = PROJ_ROOT / Path(exhibits[name]["path"])
    else:
        exhibits[name]["input_path"] = PROJ_ROOT / Path(exhibits[name]["input_path"])
        exhibits[name]["output_path"] = PROJ_ROOT / Path(exhibits[name]["output_path"])


# Auth
USERNAME = os.getenv("SAMSAPI_USERNAME")
PASSWORD = os.getenv("SAMSAPI_PASSWORD")

# ===== CACHE =====
# Geocodes
geolocator = Nominatim(user_agent="sams")
novatim_geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

if os.getenv("GOOGLE_MAPS_API_KEY") is not None:
    logger.info("Google MAPS API key found")
    google_geolocator = GoogleV3(api_key=os.getenv("GOOGLE_MAPS_API_KEY"))
    gmaps_geocode = RateLimiter(google_geolocator.geocode, min_delay_seconds=1 / 50)
else:
    logger.warning("Google MAPS API key not found, using Nominatim geocoder")
    gmaps_geocode = novatim_geocode


GEOCODES_CACHE = CACHE / "geocodes.pkl"
if "GEOCODES" not in globals():
    if os.path.exists(GEOCODES_CACHE):
        with open(GEOCODES_CACHE, "rb") as f:
            GEOCODES = pickle.load(f)
            logger.info(f"Loaded {len(GEOCODES)} geocodes from cache")

    else:
        GEOCODES = {}

# Error constants
ERRMAX = 3

# Metadata
STUDENT = {
    "PDIS": {"yearmin": 2020, "yearmax": 2024},
    "ITI": {"yearmin": 2017, "yearmax": 2024},
    "Diploma": {"yearmin": 2018, "yearmax": 2024},
    "HSS": {"yearmin": 2018, "yearmax": 2024},
    "DEG": {"yearmin": 2018, "yearmax": 2024},

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
