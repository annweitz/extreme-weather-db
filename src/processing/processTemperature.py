import xarray as xr
from processing_functions import getExistingTopTen, update_top_n
from src.config import RESULT_FOLDER

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
    temperatureLow10.to_netcdf(f"{RESULT_FOLDER}/top10temperatureLow.nc")
    temperatureLow10.close()


    temperatureDS.close()