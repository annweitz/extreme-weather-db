import logging
import sys
import subprocess
import traceback
from random import random
import cdsapi
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from src.utils.utilityFunctions import packDownloadRecords

project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))
from src.config import DOWNLOAD_FOLDER, DOWNLOAD_DATABASE, MAX_WORKERS_DOWNLOAD, PROCESSING_FOLDER, MAX_TRIES_DOWNLOADING
from downloadDatabaseFunctions import initializeDatabase, updateStatus, incrementTries, getStatus
from downloadingFactory import DownloadingFactory

FAKE_DOWNLOADS = False

# TODO: implement logging
# TODO. implement error handling
# TODO: implement factory pattern for 
    # requestBuilder
    # sanityCheck
    # download
    # updateStatus
    # incrementTries
    # getTriesByID
    # getTries
    # getStatusByID
    # getStatus
    # download_manager
    # pack_records
    # main
# TODO: implement tests
# TODO. add documenation
# TODO: look for information that users need to know to use this script

"""
This script downloads data from the Copernicus Climate Data Store (CDS) using the cdsapi library. The folders for the downloads are defined in the scratch_folder and project_folder variables. The variables to download are specified in the variables list. The script downloads the data, checks if the data is within the expected size and range, and updates the status in the database.

- The user can specify the following parameters in the script:
1. the variables to download, the years to download and file paths to the download folders. 
2. maximum number of tries for each download. If the download fails after the maximum number of tries, the status is set to failed. The default is 15.
3. maximum number of workers for the ThreadPoolExecutor. The default is 4.
4. the database file name. The default is download_database.db.
5. the range of years to download. The default is 2000 to 2023.
6. the dataset, request and file for each variable. The script already contains the dataset, request and file for the wind, temperature and precipitation variables.
7. Reasonable values for the minimum and maximum magnitude of the variables. The script already contains the same for wind, temperature and precipitation. 

- The user must also provide the expected range of file size for each variable. This script already contains the same for wind, temperature and precipitation for files containing monthly data of the wind file which contains the fields:
1. u-component of wind at 10m
2. v-component of wind at 10m
3. instantaneous 10m wind gust
The temperature file contains the field:
1. temperature at 1000hPa
The precipitation file contains the field:
1. total precipitation
2. precipitation type

The libraries used in this script are:
1. calendar - to check if the year is a leap year
2. glob - to get a list of files in a folder
3. os.path - to get the size of a file
4. subprocess - to run the merge_script.py
5. time - to get the current time
6. traceback - to get the traceback of an exception
7. zipfile - to extract files from a zip archive
8. cdsapi - to download data from the Copernicus Climate Data Store
9. sqlite3 - to connect to an SQLite database
10. random - to generate random numbers
11. xarray - to open and manipulate netCDF files
12. ThreadPoolExecutor - to run multiple download_manager functions concurrently
13. shutil - to remove a directory

"""

def download(year,month,var):

    logging.info(f"Starting download for {year}-{month}-{var}")

    client = cdsapi.Client()

    try:
        # Get request builder and sanity check function for this variable
        requestBuilder = DownloadingFactory.getRequestBuilder(var)
        sanityChecker = DownloadingFactory.getSanityChecker(var)

        dataset, request, file = requestBuilder(year, month)
        client.retrieve(dataset, request, file)

        # sanity check
        file_status = sanityChecker(file)

    except ValueError as e:
        logging.error("Invalid download type: %s - Error: %s", var, str(e))
    except Exception as e:
        logging.exception("Unexpected error while downloading %s: %s-%s - %s", var, year, month, str(e))

    return file_status


def fakeDownload(year,month,var):
    if random.randrange(100) > 1:
        return "downloaded"
    else:
        return "failed"


def download_manager(args, database = DOWNLOAD_DATABASE):
    vals = args.split(":")
    # unpack arguments
    year = int(vals[0])
    month = int(vals[1])
    var = vals[2]

    logging.info(f"Currently downloading {year}:{month}:{var}")

    connection = sqlite3.connect(database)
    cursor = connection.cursor()

    # Set status to downloading and increment tries, in case something happens while downloading
    updateStatus(year,month,var,"downloading", cursor)
    incrementTries(year,month,var,cursor)

    # Execute download and update status in database
    if(FAKE_DOWNLOADS):
        downloadStatus = fakeDownload(year, month, var)
    else:
        downloadStatus = download(year,month,var)
    updateStatus(year,month,var,downloadStatus,cursor)
    logging.info(f"Download finished for {year}:{month}:{var} - download status: {downloadStatus}")

    # Check if we can merge this year
    try:

        if(downloadStatus != "downloaded"):
            # If the download was not successful, we don't want to merge
            pass
        else:
            # Get the amount of months that have been successfully downloaded for the current year and variable
            monthsFinished = cursor.execute(f"select count(*) from downloads where year = {year} and variable = '{var}'"
                                           f"and status = 'downloaded'").fetchone()[0]
            if(monthsFinished == 12):
                # Call merge script with year and var
                arg1 = year
                arg2 = var
                logging.info(f"Running merge script for {var} year {year}")
                subprocess.run(["python", "merge_script.py", str(arg1), arg2])
    except Exception as e:
        logging.error(traceback.format_exc())
    connection.close()


def main(loop = False, fakeDownload = False):
    # Initialize logging
    logging.basicConfig(
        filename=f"{PROCESSING_FOLDER}downloading.log",  # Save logs to a file
        level=logging.INFO,  # Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
        format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
    )

    logging.info("Starting main download script")
    if(loop):
        logging.info("Looping enabled. Looping until everything is done or failed too often.")
    if(fakeDownload):
        FAKE_DOWNLOADS = fakeDownload
        logging.info("Fake downloads enabled.")
    logging.info(f"Download folder: {DOWNLOAD_FOLDER}")
    logging.info(f"Download database: {DOWNLOAD_DATABASE}")

    # establish sql connection to database
    connection = sqlite3.connect(DOWNLOAD_DATABASE)
    cursor = connection.cursor()

    # check if downloads table exists, if it doesn't, create it
    exists = cursor.execute("SELECT exists(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'downloads')").fetchone()[0]
    if exists == 0:
        initializeDatabase(connection,yearrange=[2000,2023])

    #
    while(True):
        # Get every record from the download database, where the status is not 'downloaded' and maximum number of tries has not been exceeded yet.
        res = cursor.execute(f"SELECT year,month,variable FROM downloads WHERE NOT status = 'downloaded' AND tries < {MAX_TRIES_DOWNLOADING} ORDER BY year DESC")
        records = res.fetchall()

        if(len(records) == 0):
            logging.info(f"No downloads that have either not been finished or not exceeded that maximum tries {MAX_TRIES_DOWNLOADING} remain. Stopping program.")
            break
        else:
            logging.info(f"{len(records)} downloads remaining. Starting now with {MAX_WORKERS_DOWNLOAD} workers.")
        arguments = packDownloadRecords(records)


        with ThreadPoolExecutor(max_workers=MAX_WORKERS_DOWNLOAD) as executor:
            executor.map(download_manager, arguments)
        if(not loop):
            break


if __name__ == "__main__":
    loop = False
    fake = False
    try:
        loop = sys.argv[1]
        fake = sys.argv[2]
    except:
        pass
    main(loop = loop, fakeDownload=fake)