import calendar
import glob
import os.path
import subprocess
import time
import traceback
import zipfile
import shutil
import cdsapi
import sqlite3
import random
import xarray
from concurrent.futures import ThreadPoolExecutor


scratch_folder = "/scratch/ag-schultz/esdp2/"
project_folder = "/projects/ag-schultz/"
WIND_FILESIZE_MIN = 220000000
WIND_FILESIZE_MAX = 320000000
TEMPERATURE_FILESIZE_MIN = 50000000
TEMPERATURE_FILESIZE_MAX = 90000000
PRECIPITATION_FILESIZE_MIN = 700000000
PRECIPITATION_FILESIZE_MAX = 900000000
MAX_TRIES = 15


def getDate(year, month):
    leapYear = calendar.isleap(year)
    month = int(month)
    if month in(1,3,5,7,8,10,12):
        day = 31
    elif month in (4,6,9,11):
        day = 30
    elif month == 2:
        if leapYear:
            day = 29
        else:
            day = 28
    if month < 10:
        month = f"0{month}"
    return f"{year}-{month}-01/to/{year}-{month}-{day}"

def initializeDatabase(connection, yearrange = [2000,2023], variables = ["temperature", "precipitation", "wind"]):
    cursor = connection.cursor()

    # create table
    cursor.execute(
        "CREATE TABLE downloads(id INTEGER PRIMARY KEY, year INTEGER, month INTEGER, variable TEXT, status TEXT, tries INTEGER)")

    # fill table
    for year in range(yearrange[0], yearrange[1] + 1):
        for month in range(1, 13):
            for var in variables:
                cursor.execute(f"""
                INSERT INTO downloads (id, year, month, variable, status, tries) VALUES
                (NULL, {year},{month}, '{var}' ,'unknown', 0)
                """)

    # commit results
    connection.commit()
    return

def updateStatus(year,month,var,status,cursor):
    cursor.execute(f"update downloads set status = '{status}' where year={year} and month = {month} and variable = '{var}'")
    con = cursor.connection
    con.commit()


def incrementTries(year, month, var, cursor):
    cursor.execute(f"update downloads set tries = tries + 1  where year={year} and month = {month} and variable = '{var}'")
    con = cursor.connection
    con.commit()


def getTriesByID(id, cursor):
    result = cursor.execute(f"select tries from downloads where id = {id}")
    tries = result.fetchone()[0]
    return tries

def getTries(year,month,var,cursor):
    result = cursor.execute(f"select tries from downloads where year = {year} and month = {month} and variable = '{var}'")
    tries = result.fetchone()[0]
    return tries

def getStatusByID(id, cursor):
    result = cursor.execute(f"select status from downloads where id = {id}")
    tries = result.fetchone()[0]
    return tries

def getStatus(year,month,var,cursor):
    result = cursor.execute(f"select status from downloads where year = {year} and month = {month} and variable = '{var}'")
    status = result.fetchone()[0]
    return status

def download(year,month,var):
    # build cdsapi request and unpack arguments
    try:
        dataset, request, file = requestBuilder(year, month, var)

        # client for downloading
        client = cdsapi.Client()

        # start download
        print(f"Starting download for {year}-{month}-{var}")
        client.retrieve(dataset,request, file)
    except:
        print(traceback.format_exc())
    # sanity check
    file_status = sanityCheck(file, var)
    return file_status

def requestBuilder(year, month, var):
    if (var == "temperature"):
        request =  requestBuilderTemperature(year,month)
    elif (var == "wind"):
        request = requestBuilderWind(year, month)
    elif (var == "precipitation"):
        request = requestBuilderPrecipitation(year, month)
    else:
        print("unknown variable")
    return request
def requestBuilderTemperature(year, month):
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
        "grid": [0.25,0.25],
        "format": "netcdf"}
    file = f"{scratch_folder}temperature_{year}_{month}.nc"
    return (dataset, request, file)

def requestBuilderWind(year, month):
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
        "grid": [0.25, 0.25]
        # need to download zip for now. cannot open the downloaded .nc file with xarray
        #,"format": "netcdf"
        }
    file = f"{scratch_folder}wind_{year}_{month}.zip"
    return (dataset,request,file)

def requestBuilderPrecipitation(year, month):
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
    "grid": [0.25,0.25],
    "format": "netcdf"}
    file = f"{scratch_folder}precipitation_{year}_{month}.nc"
    return (dataset,request,file)

def fakeDownload(year,month,var):
    filepath = f"{scratch_folder}{var}_{year}_{month}.nc"

    if(var == "wind"):
        filepath = filepath.replace(".nc",".zip")
    file_status = sanityCheck(filepath, var)

    return file_status

def sanityCheck(file_path, var):
    if (var == "temperature"):
        return sanityCheckTemperature(file_path)
    elif (var == "wind"):
        return sanityCheckWind(file_path)
    elif (var == "precipitation"):
        return sanityCheckPrecipitation(file_path)
    else:
        print("Unrecognized variable")

def sanityCheckTemperature(file_path):
    # Check if size is in acceptable parameters
    size = os.path.getsize(file_path)
    if (size >= TEMPERATURE_FILESIZE_MIN and size <= TEMPERATURE_FILESIZE_MAX):
        pass
    else:
        return "failed"

    # Check if min and max values are within acceptable parameters
    TEMP_MIN = 180 # -90°C
    TEMP_MAX = 340 #  67°C
    dataset = xarray.open_dataset(file_path)
    if (getMinNC(dataset,"t") >= TEMP_MIN and getMaxNC(dataset,"t") <= TEMP_MAX):
        status = "downloaded"
    else:
        status = "failed"
    dataset.close()
    return status


def sanityCheckWind(file_path):
    # Check if size is in acceptable parameters
    try:
        size = os.path.getsize(file_path)
        if (size >= WIND_FILESIZE_MIN and size <= WIND_FILESIZE_MAX):
            pass
        else:
            return "failed"
        # Check if min and max values are within acceptable parameters
        # First we need to unzip the zip archive
        tempname = f"temp_{file_path.replace(scratch_folder,'').replace('.zip','')}"
        with zipfile.ZipFile(file_path) as zipref:
            zipref.extractall(f"{scratch_folder}{tempname}/")
        files = glob.glob(scratch_folder + tempname + "/*.nc")
        merged_dataset = xarray.open_mfdataset(files)

        WIND_MIN = -150
        WIND_MAX = 150

        if (getMinNC(merged_dataset,"i10fg") >= WIND_MIN and getMaxNC(merged_dataset,"i10fg") <= WIND_MAX):
            pass
        else:
            status = "failed"

        if (getMinNC(merged_dataset,"v10") >= WIND_MIN and getMaxNC(merged_dataset,"v10") <= WIND_MAX):
            pass
        else:
            status = "failed"

        if (getMinNC(merged_dataset,"u10") >= WIND_MIN and getMaxNC(merged_dataset,"u10") <= WIND_MAX):
            status = "downloaded"
        else:
            status = "failed"
        merged_dataset.to_netcdf(file_path.replace(".zip",".nc"))
        merged_dataset.close()
    except:
        return "failed"
    try:
        #delete temp folder
        temp_path = f"{scratch_folder}/{tempname}/"
        shutil.rmtree(temp_path, ignore_errors=False)

        # delete zip folder
        os.remove(file_path)
    except:
        print(traceback.format_exc())
    return status


def sanityCheckPrecipitation(file_path):
    # Check if size is in acceptable parameters
    size = os.path.getsize(file_path)
    if (size >= PRECIPITATION_FILESIZE_MIN and size <= PRECIPITATION_FILESIZE_MAX):
        pass
    else:
        return "failed"

    # Check if min and max values are within acceptable parameters
    PRECIP_MIN = 0      #
    PRECIP_MAX = 0.45    # 450mm / 1000 to convert to m
    dataset = xarray.open_dataset(file_path)
    if (getMinNC(dataset,"tp") >= PRECIP_MIN and getMaxNC(dataset,"tp") <= PRECIP_MAX):
        status = "downloaded"
    else:
        status =  "failed"
    dataset.close()
    return status


def getMinNC(dataset, variable):
    min = dataset[variable].min().to_numpy()
    return min

def getMaxNC(dataset, variable):
    max = dataset[variable].max().to_numpy()
    return max

def download_manager(args, database = "download_database.db"):
    vals = args.split(":")
    # unpack arguments
    year = int(vals[0])
    month = int(vals[1])
    var = vals[2]

    print(f"Currently processing {year}:{month}:{var}")

    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    # check if month to download was already downloaded (should not be the case)
    status = getStatus(year,month,var,cursor)
    if(status == "downloaded"):
        print(f"{year}:{month}:{var} skipped, because it was already downloaded")
        return
    updateStatus(year,month,var,"downloading", cursor)
    incrementTries(year,month,var,cursor)
    downloadStatus = download(year,month,var)
    print(f"{year}:{month}:{var} - download status: {downloadStatus}")
    updateStatus(year,month,var,downloadStatus,cursor)

    #check if we can merge this year
    try:

        if(downloadStatus != "downloaded"):
            # don't need to check if this download wasn't successful
            pass
        else:
            yearsFinished = cursor.execute(f"select count(*) from downloads where year = {year} and variable = '{var}'"
                                           f"and status = 'downloaded'").fetchone()[0]
            print(f"years finished: {yearsFinished}")
            if(yearsFinished == 12):
                # call merge script with year and var
                arg1 = year
                arg2 = var
                print("running merge script")
                subprocess.run(["python", "merge_script.py", str(arg1), arg2])
            else:
                pass
    except:
        print(traceback.format_exc())
    connection.close()
    return status

def pack_records(records):
    arguments = []
    for row in records:
        retVal = f"{row[0]}:{row[1]}:{row[2]}"
        arguments.append(retVal)
    return arguments

def main():

    # establish sql connection to database
    connection = sqlite3.connect("download_database.db")
    cursor = connection.cursor()

    # check if downloads table exists, if it doesn't, create it
    exists = cursor.execute("Select exists(select 1 from sqlite_master where type = 'table' and name = 'downloads')").fetchone()[0]
    if exists == 0:
        initializeDatabase(connection,yearrange=[2000,2023])

    # get everything that has either not been done yet or failed
    res = cursor.execute(f"select year,month,variable from downloads where not status = 'downloaded' and tries < {MAX_TRIES} order by year desc")
    records = res.fetchall()

    arguments = pack_records(records)


    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(download_manager, arguments)
    #for result in results:
    #    print(result)

if __name__ == "__main__":
    main()