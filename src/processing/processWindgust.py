import xarray as xr
from processing_functions import getConnectedEvents, getExistingTopTen, update_top_n
from databaseFunctions import insertEventsIntoDatabase
from src.config import RESULT_FOLDER, RESULT_DATABASE

def processWindgust(datasetFilepath):
    filename = "windgustHourly"

    windgustDataset = xr.open_dataset(datasetFilepath)

    # Preprocessing:
    # drop unused coordinates: number, exp_ver
    windgustDataset = windgustDataset.drop_vars(["number", "expver"])

    # calculate events that exceed beaufort 10
    windgustThreshold = getConnectedEvents(windgustDataset, "i10fg", 24.5)

    # save events in database
    insertEventsIntoDatabase(f"{RESULT_DATABASE}", "windgustHourly", windgustThreshold)

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