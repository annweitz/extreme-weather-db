import calendar
import os.path
import cdsapi
import sqlite3
import random
import xarray
from concurrent.futures import ThreadPoolExecutor


scratch_folder = "/scratch/ag-schultz/esdp2/"
project_folder = "/projects/ag-schultz/"
WIND_FILESIZE_MIN = 0
WIND_FILESIZE_MAX = 0
TEMPERATURE_FILESIZE_MIN = 0
TEMPERATURE_FILESIZE_MAX = 0
PRECIPITATION_FILESIZE_MIN = 0
PRECIPITATION_FILESIZE_MAX = 0



def getDate(year, month):
    leapYear = calendar.isleap(year)
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

def initializeDatabase(connection, yearrange = [1960,2023], variables = ["temperature", "precipitation", "wind"]):
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
    tries = result.fetchone()[0]
    return tries

def download(year,month,var):
    # build cdsapi request and unpack arguments
    dataset, request, file = requestBuilder(year, month, var)

    # client for downloading
    client = cdsapi.Client()

    # start download
    client.retrieve(dataset,request, file)

    # sanity check
    file_status = sanityCheck(file, var)
    return file_status

def requestBuilder(year, month, var):
    if (var == "temperature"):
        return requestBuilderTemperature(year,month)
    elif (var == "wind"):
        return requestBuilderWind(year, month)
    elif (var == "precipitation"):
        return requestBuilderPrecipitation(year, month)

def requestBuilderTemperature(year, month):
    date = getDate(year, month)
    date.replace("to/", "")
    dataset = "derived-era5-pressure-levels-daily-statistics"
    request = {
        "product_type": "reanalysis",
        "variable": ["temperature"],
        "date": "1994-07-01/1994-07-31",
        "pressure_level": ["1000"],
        "daily_statistic": "daily_mean",
        "time_zone": "utc+00:00",
        "frequency": "1_hourly",
        "grid": [0.25,0.25],
        "format": "netcdf"}
    file = f"{scratch_folder}temperature{year}_{month}.nc"
    return (dataset, request, file)

def requestBuilderWind(year, month):
    date = getDate(year, month)
    date.replace("to/", "")

    dataset = "derived-era5-single-levels-daily-statistics"
    request = {
        "product_type": "reanalysis",
        "variable": [
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "instantaneous_10m_wind_gust"
        ],
        "date": "1994-07-01/1994-07-31",
        "daily_statistic": "daily_maximum",
        "time_zone": "utc+00:00",
        "frequency": "1_hourly",
        "grid": [0.25, 0.25]
        # need to download zip for now. cannot open the downloaded .nc file with xarray TODO: check why this happens, possibly fix the problem
        #,"format": "netcdf"
        }
    file = f"{scratch_folder}wind{year}_{month}.nc"
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
    if random.randrange(100)>1:
        return "downloaded"
    else:
        return "failed"


def sanityCheck(file_path, var):
    if (var == "temperature"):
        return sanityCheckTemperature(file_path)
    elif (var == "wind"):
        return sanityCheckWind(file_path)
    elif (var == "precipitation"):
        return sanityCheckPrecipitation(file_path)

def sanityCheckTemperature(file_path):
    # Check if size is in acceptable parameters
    size = os.path.getsize()
    if (size >= TEMPERATURE_FILESIZE_MIN and size <= TEMPERATURE_FILESIZE_MAX):
        pass
    else:
        return "failed"

    # Check if min and max values are within acceptable parameters
    TEMP_MIN = 0
    TEMP_MAX = 0
    if (getMinNC(file_path) >= TEMP_MIN and getMaxNC(file_path <= TEMP_MAX)):
        return "downloaded"
    else:
        return "failed"


def sanityCheckWind(file_path):
    # Check if size is in acceptable parameters
    size = os.path.getsize()
    if (size >= WIND_FILESIZE_MIN and size <= WIND_FILESIZE_MAX):
        pass
    else:
        return "failed"

    # Check if min and max values are within acceptable parameters
    # First we need to unzip the zip archive


def sanityCheckPrecipitation(file_path):
    # Check if size is in acceptable parameters
    size = os.path.getsize()
    if (size >= PRECIPITATION_FILESIZE_MIN and size <= PRECIPITATION_FILESIZE_MAX):
        pass
    else:
        return "failed"

    # Check if min and max values are within acceptable parameters


def getMinNC(file, variable):
    ncFile = xarray.open_dataset(file)
    min = ncFile[variable].min().to_numpy()
    return min

def getMaxNC(file, variable):
    ncFile = xarray.open_dataset(file)
    max = ncFile[variable].max().to_numpy()
    return max

def download_manager(args, database = "download_database.db"):
    vals = args.split(":")
    # unpack arguments
    year = vals[0]
    month = vals[1]
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
    downloadStatus = fakeDownload(year,month,var)

    updateStatus(year,month,var,downloadStatus,cursor)
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

    # check if downloads table exists, if it doesnt, create it
    exists = cursor.execute("Select exists(select 1 from sqlite_master where type = 'table' and name = 'downloads')").fetchone()[0]
    if exists == 0:
        initializeDatabase(connection)

    # get everything that has either not been done yet or failed #TODO: max amount of tries
    res = cursor.execute("select year,month,variable from downloads where status = 'unknown' or status = 'failed'")
    records = res.fetchall()

    arguments = pack_records(records)


    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(download_manager, arguments)
    #for result in results:
    #    print(result)

if __name__ == "__main__":
    main()