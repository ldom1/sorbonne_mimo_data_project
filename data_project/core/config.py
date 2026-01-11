import os
from pathlib import Path

DATA_PATH = Path(__file__).parent.parent.parent / "data"

RAW_DATA_PATH = DATA_PATH / "raw"
SILVER_DATA_PATH = DATA_PATH / "silver"
GOLD_DATA_PATH = DATA_PATH / "gold"
ANALYTICS_DATA_PATH = DATA_PATH / "analytics"

os.makedirs(RAW_DATA_PATH, exist_ok=True)
os.makedirs(SILVER_DATA_PATH, exist_ok=True)
os.makedirs(GOLD_DATA_PATH, exist_ok=True)
os.makedirs(ANALYTICS_DATA_PATH, exist_ok=True)

CARACT_FILE_RAW = RAW_DATA_PATH / "caract-2024.csv"
LIEUX_FILE_RAW = RAW_DATA_PATH / "lieux-2024.csv"
USAGERS_FILE_RAW = RAW_DATA_PATH / "usagers-2024.csv"
VEHICULES_FILE_RAW = RAW_DATA_PATH / "vehicules-2024.csv"

CARACT_FILE_SILVER = SILVER_DATA_PATH / "caract-2024.csv"
LIEUX_FILE_SILVER = SILVER_DATA_PATH / "lieux-2024.csv"
USAGERS_FILE_SILVER = SILVER_DATA_PATH / "usagers-2024.csv"
VEHICULES_FILE_SILVER = SILVER_DATA_PATH / "vehicules-2024.csv"

DATA_GOLD = GOLD_DATA_PATH / "data.csv"

PARIS_MAP_PATH = ANALYTICS_DATA_PATH / "paris_accidents_map.html"
