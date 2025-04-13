from src.utils.utilityFunctions import getDate, getMinNC, getMaxNC
from src.config import DOWNLOAD_FOLDER
from os.path import getsize
from xarray import open_dataset


WINDGUST_FILESIZE_MIN = 1000000000 # in GB (1GB)
WINDGUST_FILESIZE_MAX = 1600000000  # in GB (1.6GB)


def requestBuilderWindgust(year, month, grid):
    date = getDate(year, month)
    date = date.replace("to/", "")
    dataset = "reanalysis-era5-single-levels"
    request = {
        "product_type": ["reanalysis"],
        "date": date,
        "time": [
            "00:00", "01:00", "02:00",
            "03:00", "04:00", "05:00",
            "06:00", "07:00", "08:00",
            "09:00", "10:00", "11:00",
            "12:00", "13:00", "14:00",
            "15:00", "16:00", "17:00",
            "18:00", "19:00", "20:00",
            "21:00", "22:00", "23:00"
        ],
        "grid": grid,
        "data_format": "netcdf",
        "download_format": "unarchived",
        "variable": ["instantaneous_10m_wind_gust"]
    }
    file = f"{DOWNLOAD_FOLDER}windgust_{year}_{month}.nc"
    return (dataset, request, file)

def sanityCheckWindgust(filePath):

    size = getsize(filePath)
    if (size >= WINDGUST_FILESIZE_MIN and size <= WINDGUST_FILESIZE_MAX):
        pass
    else:
        return "failed"
    WIND_MIN = 0
    WIND_MAX = 150
    dataset = open_dataset(filePath)
    if (getMinNC(dataset, "i10fg") >= WIND_MIN and getMaxNC(dataset, "i10fg") <= WIND_MAX):
        status = "downloaded"
    else:
        status = "failed"
    dataset.close()
    return status