import xarray as xr
from processing_functions import getConnectedEvents, getExistingTopTen, update_top_n
from databaseFunctions import insertEventsIntoDatabase
from src.config import RESULT_FOLDER, RESULT_DATABASE

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
    insertEventsIntoDatabase(f"{RESULT_DATABASE}", "rainHourly", rainHourlyThreshold)

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

    insertEventsIntoDatabase(f"{RESULT_DATABASE}", "rainDaily", rainDailyThreshold)


    rainHourlyDS.close()
    rainDailyDS.close()

    snowDailyDS = snowHourlyDS.coarsen(valid_time = 24).sum()
    snowDailyThreshold = getConnectedEvents(snowDailyDS, "tp", 0.1)

    insertEventsIntoDatabase(f"{RESULT_DATABASE}", "snowDaily", snowDailyThreshold)

    snowHourlyDS.close()
    snowDailyDS.close()