from src.utils.utilityFunctions import getDate, getMinNC, getMaxNC
from src.config import DOWNLOAD_FOLDER
from os.path import getsize
from xarray import open_dataset

PRECIPITATION_FILESIZE_MIN = 700000000
PRECIPITATION_FILESIZE_MAX = 900000000


def requestBuilderPrecipitation(year, month, grid):
    date = getDate(year, month)
    dataset = "reanalysis-era5-complete"
    request = {
        "class": "ea",
        "date": date,
        "expver": "1",
        "levtype": "sfc",
        "param": "228.128/260015",
        "step": "6/7/8/9/10/11/12/13/14/15/16/17",
        "stream": "oper",
        "time": "06:00:00/18:00:00",
        "type": "fc",
        "grid": grid,
        "format": "netcdf"}
    file = f"{DOWNLOAD_FOLDER}precipitation_{year}_{month}.nc"
    return (dataset, request, file)


def sanityCheckPrecipitation(filePath):
    size = getsize(filePath)
    if (size >= PRECIPITATION_FILESIZE_MIN and size <= PRECIPITATION_FILESIZE_MAX):
        pass
    else:
        return "failed"

    PRECIP_MIN = 0  #
    PRECIP_MAX = 0.45  # 450mm / 1000 to convert to m
    dataset = open_dataset(filePath)
    if (getMinNC(dataset, "tp") >= PRECIP_MIN and getMaxNC(dataset, "tp") <= PRECIP_MAX):
        status = "downloaded"
    else:
        status = "failed"
    dataset.close()
    return status