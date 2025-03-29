# config.py
import os
from pathlib import Path

# Folder paths
CONFIG_DIR = Path(__file__).parent
PROJECT_ROOT = CONFIG_DIR.parent

PROCESSING_FOLDER = "/projects/ag-schultz/"
PROCESSING_DATABASE = f"{PROCESSING_FOLDER}processing.db"

RESULT_FOLDER = PROJECT_ROOT / "results/"
RESULT_DATABASE = RESULT_FOLDER / "results.db"
RESULT_TABLENAME = "thresholdResults"

DOWNLOAD_FOLDER = "/scratch/ag-schultz/"
DOWNLOAD_DATABASE = f"{DOWNLOAD_FOLDER}download_database.db"

MAX_WORKERS_DOWNLOAD = 4
MAX_WORKERS_PROCESSING = 1

METADATA = PROJECT_ROOT / "metadata.yaml"