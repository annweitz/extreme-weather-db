import sqlite3
import numpy as np
import xarray as xr
from databaseFunctions import createProcessingDatabase, createResultDatabase, insertEventsIntoDatabase, updateProcessingStatus
from processing_functions import getConnectedEvents, update_top_n, getExistingTopTen
from concurrent.futures import ThreadPoolExecutor
import logging

PROCESSING_FOLDER = "/projects/ag-schultz/"
PROCESSING_DB = f"{PROCESSING_FOLDER}processing.sql"
RESULT_FOLDER = "/projects/ag-schultz//results/"
RESULT_DB = f"{RESULT_FOLDER}results.sql"
MAX_WORKERS = 1

logging.basicConfig(
    filename="processing.log",  # Save logs to a file
    level=logging.INFO,  # Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
    format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
)

class ProcessingFactory:

    processingFunctions = {}

    @classmethod
    def registerProcessor(cls, variableName, function):
        cls.processingFunctions[variableName] = function

    @classmethod
    def getProcessor(cls, variableName):
        function = cls.processingFunctions.get(variableName)
        if not function:
            raise ValueError(f"No processor found for variable {variableName}")
        return function

ProcessingFactory.registerProcessor("precipitation", lambda path: processPrecipitation(path))
ProcessingFactory.registerProcessor("temperature", lambda path: processTemperature(path))
ProcessingFactory.registerProcessor("wind", lambda path: processWind(path))
ProcessingFactory.registerProcessor("windgust", lambda path: processWindgust(path))



def processPrecipitation(datasetPath):
    # Define ptype values for rain and snow
    rain_types = [1, 3, 7, 12]  # Rain, Freezing Rain, Rain-Snow Mix, Freezing Drizzle
    snow_types = [5, 6, 8]      # Snow, Wet Snow, Ice Pellets

    precipDataset = xr.open_dataset(datasetPath)

    # preprocessing:
    # - drop unsused vars: number, expver
    precipDataset = precipDataset.drop_vars(["number","expver"])

    # calculate a hourly dataset, that only contains rain precipitation
    rainHourlyMask = precipDataset.ptype.isin(rain_types)
    rainHourlyDS = precipDataset.copy()
    rainHourlyDS['tp'] = precipDataset.tp.where(rainHourlyMask, 0)
    rainHourlyDS = rainHourlyDS.drop_vars(["ptype"])

    # calculate a hourly dataset, that only contains snow precipitation
    snowHourlyMask = precipDataset.ptype.isin(snow_types)
    snowHourlyDS = precipDataset.copy()
    snowHourlyDS['tp'] = precipDataset.tp.where(snowHourlyMask, 0)
    snowHourlyDS = snowHourlyDS.drop_vars(["ptype"])

    # calculcate events where hourly rain exceeds 0.1m
    rainHourlyThreshold = getConnectedEvents(rainHourlyDS, "tp", 0.1)

    # Insert events into results database
    insertEventsIntoDatabase(f"{RESULT_DB}", "rainHourly", rainHourlyThreshold)

    # Drop ptype to save time and computational space, we don't need it for these calculations
    precipDataset = precipDataset.drop_vars(["ptype"])
    # Coarsen dataset to 24 hour intervals for daily processing with summed up precipitation values
    precipDailyDS = precipDataset.coarsen(valid_time = 24).sum()
    # If it exists, get current top 10 file
    currentTopTenDS = getExistingTopTen(RESULT_FOLDER, "precipitation")

    # Update top 10
    precipDailyTop10 = update_top_n(precipDailyDS, "tp", oldTop10=currentTopTenDS)

    if currentTopTenDS:
        currentTopTenDS.close()

    precipDailyTop10.to_netcdf(f"{RESULT_FOLDER}/top10precipitation.nc")
    precipDailyTop10.close()

    # Close both precipitation datasets, we dont need them after this
    precipDailyDS.close()
    precipDataset.close()

    rainDailyDS  = rainHourlyDS.coarsen(valid_time = 24).sum()
    rainDailyThreshold = getConnectedEvents(rainDailyDS, "tp", 0.2)

    insertEventsIntoDatabase(f"{RESULT_DB}", "rainDaily", rainDailyThreshold)


    rainHourlyDS.close()
    rainDailyDS.close()

    snowDailyDS = snowHourlyDS.coarsen(valid_time = 24).sum()
    snowDailyThreshold = getConnectedEvents(snowDailyDS, "tp", 0.1)

    insertEventsIntoDatabase(f"{RESULT_DB}", "snowDaily", snowDailyThreshold)

    snowHourlyDS.close()
    snowDailyDS.close()


def processTemperature(datasetPath):

    # open dataset
    temperatureDS = xr.open_dataset(datasetPath)

    # Preprocessing:
    # - select pressure level
    # - drop pressure level coordinate and useless number coordinate
    temperatureDS = temperatureDS.isel(pressure_level = 0).drop_vars(["pressure_level", "number"])


    # Get current top 10, if it already exists
    currentTopTenDS = getExistingTopTen(RESULT_FOLDER, "temperatureHigh")

    # Get top 10 highest values
    temperatureTop10 = update_top_n(temperatureDS, "t",oldTop10= currentTopTenDS ,highest=True)

    if currentTopTenDS:
        currentTopTenDS.close()

    # save and close
    temperatureTop10.to_netcdf(f"{RESULT_FOLDER}/top10temperatureHigh.nc")
    temperatureTop10.close()


    # Get current top 10, if it already exists
    currentTopTenDS = getExistingTopTen(RESULT_FOLDER, "temperatureLow")

    # Get low 10
    temperatureLow10 = update_top_n(temperatureDS, "t", oldTop10= currentTopTenDS, highest=False)

    if currentTopTenDS:
        currentTopTenDS.close()

    # save and close
    temperatureTop10.to_netcdf(f"{RESULT_FOLDER}/top10temperatureLow.nc")
    temperatureTop10.close()


    temperatureDS.close()

def processWind(datasetFilepath):

    filename = "wind"
    # open dataset
    windDataset = xr.open_dataset(datasetFilepath)

    # Preprocessing:
    # - combine u and v components to form windspeed
    # - drop not needed variables: u,v, i10fg and number

    windspeed = np.sqrt(windDataset["u10"]**2 + windDataset["v10"]**2)
    windDataset["windspeed"] = windspeed
    windDataset = windDataset.drop_vars(["v10","u10","i10fg", "number"])

    # Get current top 10, if it already exists
    currentTopTenDS = getExistingTopTen(RESULT_FOLDER, filename)

    # get new top 10
    windTop10 = update_top_n(windDataset, "windspeed", oldTop10=currentTopTenDS)

    # close old top 10 file
    if currentTopTenDS:
       currentTopTenDS.close()

    windTop10.to_netcdf(f"{RESULT_FOLDER}top10{filename}.nc")
    windTop10.close()

    # threshold beaufort 9
    windspeedThreshold = getConnectedEvents(windDataset, "windspeed", 20.8)
    insertEventsIntoDatabase(RESULT_DB, "windspeedDaily", windspeedThreshold)

    windDataset.close()



def processWindgust(datasetFilepath):
    filename = "windgustHourly"

    windgustDataset = xr.open_dataset(datasetFilepath)

    # Preprocessing:
    # drop unused coordinates: number, exp_ver
    windgustDataset = windgustDataset.drop_vars(["number", "expver"])

    # calculate events that exceed beaufort 10
    windgustThreshold = getConnectedEvents(windgustDataset, "i10fg", 24.5)

    # save events in database
    insertEventsIntoDatabase(f"{RESULT_DB}", "windgustHourly", windgustThreshold)

    '''
    # Top 10
    # Get current top 10, if it already exists
    currentTopTenDS = getExistingTopTen(RESULT_FOLDER, filename)

    # get new top 10
    windTop10 = update_top_n(windgustDataset, "i10fg", oldTop10=currentTopTenDS)

    # close old top 10 file
    if currentTopTenDS:
       currentTopTenDS.close()

    # save results and close everything
    windTop10.to_netcdf(f"{RESULT_FOLDER}top10{filename}.nc")
    windTop10.close()
    '''

    windgustDataset.close()


def processingManager(arguments):
    logging.info("Processing manager started for %s", arguments)
    year,var = arguments.split(":")
    filepath = f"{PROCESSING_FOLDER}{var}_{year}.nc"

    try:
        processor = ProcessingFactory.getProcessor(var)
        updateProcessingStatus(PROCESSING_DB,year, var, "processing")
        processor(filepath)
    except ValueError as e:
        logging.error("Invalid processing type: %s - Error: %s", arguments, str(e))
    except Exception as e:
        logging.exception("Unexpected error while processing %s - %s", arguments, str(e))

    updateProcessingStatus(PROCESSING_DB,year, var, "processed")
    logging.info("Processing finished for %s", arguments)

def pack_records(records):
    arguments = []
    for row in records:
        retVal = f"{row[0]}:{row[1]}"
        arguments.append(retVal)
    return arguments

def main():
    logging.info("Starting main processing script")
    # establish sql connection to database
    connection = sqlite3.connect(PROCESSING_DB)
    cursor = connection.cursor()

    exists = cursor.execute("Select exists(select 1 from sqlite_master where type = 'table' and name = 'processing')").fetchone()[0]
    if exists == 0:
        logging.info("Processing database does not exist yet. Creating it...")
        createProcessingDatabase(PROCESSING_FOLDER, PROCESSING_DB)

    # get everything that has either not been done yet or failed
    res = cursor.execute(f"select year,variable from processing where not status = 'processed' order by year desc")
    records = res.fetchall()

    cursor.close()
    connection.close()

    arguments = pack_records(records)
    createResultDatabase(RESULT_DB)


    logging.info("Starting parallel processing with %d workers.", MAX_WORKERS)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(processingManager, arguments)
    logging.info("Processing completed successfully.")

if __name__ == "__main__":
    main()

