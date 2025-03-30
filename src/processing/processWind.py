import xarray as xr
from processing_functions import getConnectedEvents, getExistingTopTen, update_top_n
from databaseFunctions import insertEventsIntoDatabase
from src.config import RESULT_FOLDER, RESULT_DATABASE
import numpy as np

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
    insertEventsIntoDatabase(RESULT_DATABASE, "windspeedDaily", windspeedThreshold)

    windDataset.close()