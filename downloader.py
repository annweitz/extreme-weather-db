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
import logging

# TODO. implement error handling
# TODO: implement factory pattern for 
    # requestBuilder
    # sanityCheck
    # download
    # updateStatus
    # incrementTries
    # getTriesByID
    # getTries
    # getStatusByID
    # getStatus
    # download_manager
    # pack_records
    # main
# TODO: implement tests
# TODO. add documenation
# TODO: look for information that users need to know to use this script

"""
This script downloads data from the Copernicus Climate Data Store (CDS) using the cdsapi library. The folders for the downloads are defined in the scratch_folder and project_folder variables. The variables to download are specified in the variables list. The script downloads the data, checks if the data is within the expected size and range, and updates the status in the database.

- The user can specify the following parameters in the script:
1. the variables to download, the years to download and file paths to the download folders. 
2. maximum number of tries for each download. If the download fails after the maximum number of tries, the status is set to failed. The default is 15.
3. maximum number of workers for the ThreadPoolExecutor. The default is 4.
4. the database file name. The default is download_database.db.
5. the range of years to download. The default is 2000 to 2023.
6. the dataset, request and file for each variable. The script already contains the dataset, request and file for the wind, temperature and precipitation variables.
7. Reasonable values for the minimum and maximum magnitude of the variables. The script already contains the same for wind, temperature and precipitation. 

- The user must also provide the expected range of file size for each variable. This script already contains the same for wind, temperature and precipitation for files containing monthly data of the wind file which contains the fields:
1. u-component of wind at 10m
2. v-component of wind at 10m
3. instantaneous 10m wind gust
The temperature file contains the field:
1. temperature at 1000hPa
The precipitation file contains the field:
1. total precipitation

The libraries used in this script are:
1. calendar - to check if the year is a leap year
2. glob - to get a list of files in a folder
3. os.path - to get the size of a file
4. subprocess - to run the merge_script.py
5. time - to get the current time
6. traceback - to get the traceback of an exception
7. zipfile - to extract files from a zip archive
8. cdsapi - to download data from the Copernicus Climate Data Store
9. sqlite3 - to connect to an SQLite database
10. random - to generate random numbers
11. xarray - to open and manipulate netCDF files
12. ThreadPoolExecutor - to run multiple download_manager functions concurrently
13. shutil - to remove a directory

"""

# configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# file paths to temp and project folders
scratch_folder = "/scratch/ag-schultz/esdp2/"
project_folder = "/projects/ag-schultz/"

# check if directories exist, if not create them
if not os.path.exists(scratch_folder):
    os.makedirs(scratch_folder)
    logger.info(f"Created directory: {scratch_folder}")
else:
    logger.info(f"Directory already exists: {scratch_folder}")

if not os.path.exists(project_folder):
    os.makedirs(project_folder)
    logger.info(f"Created directory: {project_folder}")
else:
    logger.info(f"Directory already exists: {project_folder}")

# file size limits
WIND_FILESIZE_MIN = 220000000
WIND_FILESIZE_MAX = 320000000
TEMPERATURE_FILESIZE_MIN = 50000000
TEMPERATURE_FILESIZE_MAX = 90000000
PRECIPITATION_FILESIZE_MIN = 700000000
PRECIPITATION_FILESIZE_MAX = 900000000
WINDGUST_FILESIZE_MIN = 1000000000 # in GB (1GB)
WINDGUST_FILESIZE_MAX = 1600000000  # in GB (1.6GB)
# maximum number of tries for each download
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

def initializeDatabase(connection, yearrange = [2000,2023], variables = ["temperature", "precipitation", "wind", "windgust"]):
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
    cursor.execute(f"UPDATE downloads SET status = '{status}' WHERE year={year} AND month = {month} AND variable = '{var}'")
    con = cursor.connection
    con.commit()

def incrementTries(year, month, var, cursor):
    cursor.execute(f"UPDATE downloads SET tries = tries + 1  WHERE year={year} AND month = {month} AND variable = '{var}'")
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

def getMinNC(dataset, variable):
    min = dataset[variable].min().to_numpy()
    return min

def getMaxNC(dataset, variable):
    max = dataset[variable].max().to_numpy()
    return max

def download(year,month,var):
    
    # build cdsapi request and unpack arguments
    try:
        dataset, request, file = requestBuilder(year, month, var)

        # client for downloading
        client = cdsapi.Client()
        # start download
        print(f"Starting download for {year}-{month}-{var}")
        client.retrieve(dataset, request, file)
    except Exception as e:
        print(traceback.format_exc())
    # sanity check
    file_status = sanityCheck(file, var)
    return file_status

class RequestBuilderBase:
    def __init__(self, grid=[0.25,0.25], year_range=[2000,2023]):
        self.grid = grid
        self.year_range = year_range

    def build_request(self, year, month):
        raise NotImplementedError
    
class TemperatureRequestBuilder(RequestBuilderBase):
    def build_request(self, year, month):
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
            "grid": self.grid,
            "format": "netcdf"}
        file = f"{scratch_folder}temperature_{year}_{month}.nc"
        return (dataset, request, file)
    
class WindRequestBuilder(RequestBuilderBase):
    def build_request(self, year, month):
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
        "grid": self.grid
        # need to download zip for now. cannot open the downloaded .nc file with xarray
        #,"format": "netcdf"
        }
        file = f"{scratch_folder}wind_{year}_{month}.zip"
        return (dataset,request,file)
    
class PrecipitationRequestBuilder(RequestBuilderBase):
    def build_request(self, year, month):
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
        "grid": self.grid,
        "format": "netcdf"}
        file = f"{scratch_folder}precipitation_{year}_{month}.nc"
        return (dataset,request,file)
    
class WindGustRequestBuilder(RequestBuilderBase):
    def build_request(self, year, month):
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
            "grid": self.grid,
            "data_format": "netcdf",
            "download_format": "unarchived",
            "variable": ["instantaneous_10m_wind_gust"]
        }
        file = f"{scratch_folder}windgust_{year}_{month}.nc"
        return (dataset,request,file)

class RequestBuilderFactory:
    def __init__(self, grid=[0.25,0.25], year_range=[2000,2023]):
        self.grid = grid
        self.year_range = year_range

    def get_request_builder(self, var):
        if (var == "temperature"):
            return TemperatureRequestBuilder(self.grid, self.year_range)
        elif (var == "wind"):
            return WindRequestBuilder(self.grid, self.year_range)
        elif (var == "precipitation"):
            return PrecipitationRequestBuilder(self.grid, self.year_range)
        elif (var == "windgust"):
            return WindGustRequestBuilder(self.grid, self.year_range)
        else:
            raise ValueError("Unknown variable: " + var)

def requestBuilder(year, month, var):
    factory = RequestBuilderFactory(grid=[0.25,0.25], year_range=[2000,2023])
    builder = factory.get_request_builder(var)
    return builder.build_request(year, month)

def fakeDownload(year,month,var):
    filepath = f"{scratch_folder}{var}_{year}_{month}.nc"

    if(var == "wind"):
        filepath = filepath.replace(".nc",".zip")
    file_status = sanityCheck(filepath, var)

    return file_status

class SanityCheckBase:
    def check(self,file_path):
        raise NotImplementedError('Implement method for sanity check of variable')
    
class TemperatureSanityCheck(SanityCheckBase):
    def check(self,file_path):
        size = os.path.getsize(file_path)
        if (size >= TEMPERATURE_FILESIZE_MIN and size <= TEMPERATURE_FILESIZE_MAX):
            pass
        else:
            logger.info(f"Temperature file size out of range: {size}")
            return "failed"

        TEMP_MIN = 180 # -90°C
        TEMP_MAX = 340 #  67°C
        dataset = xarray.open_dataset(file_path)
        if (getMinNC(dataset,"t") >= TEMP_MIN and getMaxNC(dataset,"t") <= TEMP_MAX):
            status = "downloaded"
        else:
            logger.info(f"Temperature range out of bounds: {getMinNC(dataset,'t')} - {getMaxNC(dataset,'t')}")
            status = "failed"
        dataset.close()
        return status
    
class WindSanityCheck(SanityCheckBase):
    def check(self,file_path):
        size = os.path.getsize(file_path)
        if (size >= WIND_FILESIZE_MIN and size <= WIND_FILESIZE_MAX):
            pass
        else:
            logger.info(f"Wind file size out of range: {size}")
            return "failed"
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
            logger.info(f"Wind gust range out of bounds: {getMinNC(merged_dataset,'i10fg')} - {getMaxNC(merged_dataset,'i10fg')}")
            status = "failed"

        if (getMinNC(merged_dataset,"v10") >= WIND_MIN and getMaxNC(merged_dataset,"v10") <= WIND_MAX):
            pass
        else:
            logger.info(f"v10 range out of bounds: {getMinNC(merged_dataset,'v10')} - {getMaxNC(merged_dataset,'v10')}")
            status = "failed"

        if (getMinNC(merged_dataset,"u10") >= WIND_MIN and getMaxNC(merged_dataset,"u10") <= WIND_MAX):
            status = "downloaded"
        else:
            logger.info(f"u10 range out of bounds: {getMinNC(merged_dataset,'u10')} - {getMaxNC(merged_dataset,'u10')}")
            status = "failed"
        merged_dataset.to_netcdf(file_path.replace(".zip",".nc"))
        merged_dataset.close()
        try:
            temp_path = f"{scratch_folder}/{tempname}/"
            shutil.rmtree(temp_path, ignore_errors=False)
            os.remove(file_path)
        except:
            print(traceback.format_exc())
        return status
    
class PrecipitationSanityCheck(SanityCheckBase):
    def check(self,file_path):
        size = os.path.getsize(file_path)
        if (size >= PRECIPITATION_FILESIZE_MIN and size <= PRECIPITATION_FILESIZE_MAX):
            pass
        else:
            logger.info(f"Precipitation file size out of range: {size}")
            return "failed"

        PRECIP_MIN = 0      #
        PRECIP_MAX = 0.45    # 450mm / 1000 to convert to m
        dataset = xarray.open_dataset(file_path)
        if (getMinNC(dataset,"tp") >= PRECIP_MIN and getMaxNC(dataset,"tp") <= PRECIP_MAX):
            status = "downloaded"
        else:
            logger.info(f"Precipitation range out of bounds: {getMinNC(dataset,'tp')} - {getMaxNC(dataset,'tp')}")
            status =  "failed"
        dataset.close()
        return status

class WindGustSanityCheck(SanityCheckBase):
    def check(self,file_path):
        size = os.path.getsize(file_path)
        if (size >= WINDGUST_FILESIZE_MIN and size <= WINDGUST_FILESIZE_MAX):
            pass
        else:
            logger.info(f"Wind gust file size out of range: {size}")
            return "failed"
        WIND_MIN = 0
        WIND_MAX = 150
        dataset = xarray.open_dataset(file_path)
        if (getMinNC(dataset,"i10fg") >= WIND_MIN and getMaxNC(dataset,"i10fg") <= WIND_MAX):
            status = "downloaded"
        else:
            logger.info(f"Wind gust range out of bounds: {getMinNC(dataset,'i10fg')} - {getMaxNC(dataset,'i10fg')}")
            status = "failed"
        dataset.close()
        return status    
    
class SanityCheckFactory:
    def get_sanity_check(self, var):
        if (var == "temperature"):
            return TemperatureSanityCheck()
        elif (var == "wind"):
            return WindSanityCheck()
        elif (var == "precipitation"):
            return PrecipitationSanityCheck()
        elif (var == "windgust"):
            return WindGustSanityCheck()
        else:
            raise ValueError("Unsupported variable for sanity check: " + var)


def sanityCheck(file_path, var):
    sanity_check_factory = SanityCheckFactory()
    sanity_check = sanity_check_factory.get_sanity_check(var)
    return sanity_check.check(file_path)

def download_manager(args, database = "/projects/ag-schultz/download_database.db"):
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
    connection = sqlite3.connect(f"{project_folder}download_database.db")
    cursor = connection.cursor()

    # check if downloads table exists, if it doesn't, create it
    exists = cursor.execute("Select exists(select 1 from sqlite_master where type = 'table' and name = 'downloads')").fetchone()[0]
    if exists == 0:
        initializeDatabase(connection,yearrange=[2000,2023])

    # get everything that has either not been done yet or failed
    res = cursor.execute(f"select year,month,variable from downloads where not status = 'downloaded' and tries < {MAX_TRIES} order by year desc")
    records = res.fetchall()

    arguments = pack_records(records)

    with ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(download_manager, arguments)
    #for result in results:
    #    print(result)

if __name__ == "__main__":
    main()