from src.utils.utilityFunctions import getDate, getMinNC, getMaxNC
from src.config import DOWNLOAD_FOLDER
from os.path import getsize
from xarray import open_dataset


TEMPERATURE_FILESIZE_MIN = 50000000
TEMPERATURE_FILESIZE_MAX = 90000000
TEMP_MIN = 180  # -90°C
TEMP_MAX = 340  # 67°C
def requestBuilderTemperature(year, month, grid):
    date = getDate(year, month)
    date = date.replace("to/", "")
    dataset = "derived-era5-pressure-levels-daily-statistics"
    request = {
        "product_type": "reanalysis",
        "variable": ["temperature"],
        "date": date,
        "pressure_level": ["1000"],
        "daily_statistic": "daily_mean",
        "time_zone": "utc+00:00",
        "frequency": "1_hourly",
        "grid": grid,
        "format": "netcdf"}
    file = f"{DOWNLOAD_FOLDER}temperature_{year}_{month}.nc"
    return (dataset, request, file)


def sanityCheckTemperature(filePath):
    size = getsize(filePath)
    if (size >= TEMPERATURE_FILESIZE_MIN and size <= TEMPERATURE_FILESIZE_MAX):
        pass
    else:
        return "failed"

    dataset = open_dataset(filePath)
    if (getMinNC(dataset, "t") >= TEMP_MIN and getMaxNC(dataset, "t") <= TEMP_MAX):
        status = "downloaded"
    else:
        status = "failed"
    dataset.close()
    return status