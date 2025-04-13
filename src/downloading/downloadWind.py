from src.utils.utilityFunctions import getDate, getMinNC, getMaxNC
from src.config import DOWNLOAD_FOLDER
import os
from xarray import open_mfdataset
from shutil import rmtree
from traceback import format_exc
import zipfile
from glob import glob

WIND_FILESIZE_MIN = 220000000
WIND_FILESIZE_MAX = 320000000


def requestBuilderWind(year, month, grid):
    date = getDate(year, month)
    date = date.replace("to/", "")

    dataset = "derived-era5-single-levels-daily-statistics"
    request = {
        "product_type": "reanalysis",
        "variable": [
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "instantaneous_10m_wind_gust"
        ],
        "date": date,
        "daily_statistic": "daily_maximum",
        "time_zone": "utc+00:00",
        "frequency": "1_hourly",
        "grid": grid
        # need to download zip for now. cannot open the downloaded .nc file with xarray
        # ,"format": "netcdf"
    }
    file = f"{DOWNLOAD_FOLDER}wind_{year}_{month}.zip"
    return (dataset, request, file)


def sanityCheckWind(filePath):
    size = os.path.getsize(filePath)
    if (size >= WIND_FILESIZE_MIN and size <= WIND_FILESIZE_MAX):
        pass
    else:
        return "failed"
    tempname = f"temp_{filePath.replace(DOWNLOAD_FOLDER, '').replace('.zip', '')}"
    with zipfile.ZipFile(filePath) as zipref:
        zipref.extractall(f"{DOWNLOAD_FOLDER}{tempname}/")
    files = glob(DOWNLOAD_FOLDER + tempname + "/*.nc")
    merged_dataset = open_mfdataset(files)

    WIND_MIN = -150
    WIND_MAX = 150

    if (getMinNC(merged_dataset, "i10fg") >= WIND_MIN and getMaxNC(merged_dataset, "i10fg") <= WIND_MAX):
        pass
    else:
        status = "failed"

    if (getMinNC(merged_dataset, "v10") >= WIND_MIN and getMaxNC(merged_dataset, "v10") <= WIND_MAX):
        pass
    else:
        status = "failed"

    if (getMinNC(merged_dataset, "u10") >= WIND_MIN and getMaxNC(merged_dataset, "u10") <= WIND_MAX):
        status = "downloaded"
    else:
        status = "failed"
    merged_dataset.to_netcdf(filePath.replace(".zip", ".nc"))
    merged_dataset.close()
    try:
        temp_path = f"{DOWNLOAD_FOLDER}/{tempname}/"
        rmtree(temp_path, ignore_errors=False)
        os.remove(filePath)
    except:
        print(format_exc())
    return status