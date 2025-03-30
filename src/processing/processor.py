import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))
import sqlite3
from databaseFunctions import createProcessingDatabase, createResultDatabase, updateProcessingStatus, updateProcessingDatabase
from concurrent.futures import ThreadPoolExecutor
import logging
from src.config import PROCESSING_FOLDER, PROCESSING_DATABASE, RESULT_FOLDER, RESULT_DATABASE, MAX_WORKERS_PROCESSING
from processingFactory import ProcessingFactory

def processingManager(arguments):
    logging.info("Processing manager started for %s", arguments)
    year,var = arguments.split(":")
    filepath = f"{PROCESSING_FOLDER}{var}_{year}.nc"

    try:
        processor = ProcessingFactory.getProcessor(var)
        updateProcessingStatus(PROCESSING_DATABASE,year, var, "processing")
        processor(filepath)
        updateProcessingStatus(PROCESSING_DATABASE, year, var, "processed")
        logging.info("Processing finished for %s", arguments)
    except ValueError as e:
        logging.error("Invalid processing type: %s - Error: %s", arguments, str(e))
    except Exception as e:
        logging.exception("Unexpected error while processing %s - %s", arguments, str(e))


def pack_records(records):
    arguments = []
    for row in records:
        retVal = f"{row[0]}:{row[1]}"
        arguments.append(retVal)
    return arguments

def main():
    # Initialize logging
    logging.basicConfig(
        filename=f"{PROCESSING_FOLDER}processing.log",  # Save logs to a file
        level=logging.INFO,  # Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
        format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
    )

    logging.info("Starting main processing script")
    logging.info(f"Processing folder: {PROCESSING_FOLDER}")
    logging.info(f"Processing database: {PROCESSING_DATABASE}")
    logging.info(f"Result folder: {RESULT_FOLDER}")
    logging.info(f"Result db: {RESULT_DATABASE}")

    # establish sql connection to database
    connection = sqlite3.connect(PROCESSING_DATABASE)
    cursor = connection.cursor()

    exists = cursor.execute("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'processing')").fetchone()[0]
    if exists == 0:
        logging.info("Processing database does not exist yet. Creating it...")
        createProcessingDatabase(PROCESSING_FOLDER, PROCESSING_DATABASE)
    else:
        logging.info("Processing database table already exists. Updating records for new .nc files.")
        numNewFiles = updateProcessingDatabase(PROCESSING_FOLDER, PROCESSING_DATABASE)
        logging.info(f"{numNewFiles} added to processing database.")

    # get everything that has either not been done yet or failed
    res = cursor.execute(f"SELECT year,variable FROM processing WHERE NOT status = 'processed' ORDER BY year DESC")
    records = res.fetchall()

    cursor.close()
    connection.close()

    arguments = pack_records(records)
    createResultDatabase(RESULT_DATABASE)


    logging.info("Starting parallel processing with %d workers.", MAX_WORKERS_PROCESSING)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_PROCESSING) as executor:
        executor.map(processingManager, arguments)
    logging.info("Processing completed successfully.")

if __name__ == "__main__":
    main()

