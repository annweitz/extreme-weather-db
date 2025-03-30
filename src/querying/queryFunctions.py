import glob
from xarray import open_dataset
from geopy.geocoders import Nominatim
from math import floor
from src.config import RESULT_FOLDER, RESULT_DATABASE, RESULT_TABLENAME
import sqlite3
from src.processing.databaseFunctions import resultDatabaseRecordsToDataframe
import pandas as pd

# function to get latitude and longitude from a city name via Nominatim and geopy
def get_lat_lon(city):
    """
    Uses Nominatim and geopy to resolve a city name to latitude and longitude
    :param city: A city name
    :return: A tuple of the corresponding latitude and longitude or None,None if the name was not found
    """
    geolocator = Nominatim(user_agent="extreme-weather-db")
    location = geolocator.geocode(city)
    if location:
        return location.latitude, location.longitude
    else:
        return None,None


def geoRound(i):
    """
    Rounds a number down to it's nearest .25 step.
    :param i: A number.
    :return: The rounded down number
    """
    return floor(i * 4) / 4


def getCityCoords(cityName):
    """
    Given a cities name, gets the latitude and longitude for the city, converts it to 0° to 360° and rounds them down to the nearest 0.25.
    :param cityName: A city name
    :return: Corresponding latitude and longitude
    """
    lat,lon = get_lat_lon(cityName)
    if (lat == None or lon == None):
        raise Exception("No city of this name found")
    else:
        # Need to convert longitude from -180 to 180 used by Nominatim to 0 to 360 ° east used by ERA5 data
        lon = lon + 360 if lon < 0 else lon
        lat = geoRound(lat)
        lon = geoRound(lon)
    return lat,lon


def getTop10City(dataset,cityname):
    """
    Gets the top 10 for a given dataset and cityname.
    :param dataset: The dataset that contains all top tens.
    :param cityname: The cityname.
    :return: A dataframe containing the ranks (1-10), values and timestamps for top 10 events.
    """
    lat,lon = getCityCoords(cityname)
    top10DF = dataset.sel(latitude = lat, longitude = lon).to_dataframe()
    return top10DF.drop(["latitude","longitude"],axis = 1)


def getTop10Datasets(resultFolder = RESULT_FOLDER):
    """
    Searches the result folder for all .nc files containing 'top10' in the filename.
    :param resultFolder: The folder that contains the results. Default: The filepath specified in the config
    :return: An array containing opened datasets for all top ten results that are present in the result folder.
    """
    datasets = [[x.split("top10")[1].replace(".nc", ""),open_dataset(x)] for x in glob.glob(RESULT_FOLDER + f"/top10*.nc")]
    return datasets


def getAllTopTensForCity(cityname):
    """
    Given a city name, gets the top ten values for all event types for this city.
    :param cityname: The city name
    :return: An array containing tuples of the event type and the corresponding top ten values and timestamps.
    """
    top10ResultsCity = []

    top10Data = getTop10Datasets(RESULT_FOLDER)
    for top10 in top10Data:
        name = top10[0]
        dataset = top10[1]
        datasetResult = getTop10City(dataset, cityname)
        top10ResultsCity.append([name, datasetResult])

    return top10ResultsCity


def getTopTenForCityForEventType(cityname, eventType):
    """
    Given a city name and an event type, returns the top ten for that city and event
    :param cityname: A city name
    :param eventType: The event type to open
    :return: A dataframe containing the top ten values and timestamps
    """
    try:
        dataset = open_dataset(RESULT_FOLDER + f"top10{eventType}.nc")
        return getTop10City(dataset, cityname)
    except FileNotFoundError:
        raise FileNotFoundError





def getAllRecordsForCity(cityname, resultDatabase = RESULT_DATABASE, tableName = "thresholdResults") -> pd.DataFrame :
    """
     Given a city and an eventType, returns all records that occurred in the cities grid-box.
     :param cityname: The city name
     :param resultDatabase: Path to the result database. Defaults to the path in the config.
     :param tableName: Tablename for the table in the result database. Defaults to the table specified in the config.
     :return: A dataframe containing all records for the query.
     """
    connection = sqlite3.connect(resultDatabase)
    cursor = connection.cursor()

    lat, lon = getCityCoords(cityname)

    records = cursor.execute(f"SELECT * FROM {tableName} "
                   f"WHERE minLatitude <= {lat} "
                   f"AND maxLatitude >= {lat} "
                   f"AND minLongitude <= {lon} "
                   f"AND maxLongitude >= {lon}")

    results = records.fetchall()

    cursor.close()
    connection.close()

    return resultDatabaseRecordsToDataframe(results)

def getAllRecordsForCityAndEventType(cityname,eventType, resultDatabase = RESULT_DATABASE, tableName = RESULT_TABLENAME) -> pd.DataFrame:
    """
    Given a city and an eventType, returns all records that occurred for this event in the cities grid-box.
    :param cityname: The city name
    :param eventType: The event type
    :param resultDatabase: Path to the result database. Defaults to the path in the config.
    :param tableName: Tablename for the table in the result database. Defaults to the table specified in the config.
    :return: A dataframe containing all records for the query.
    """

    connection = sqlite3.connect(resultDatabase)
    cursor = connection.cursor()

    lat, lon = getCityCoords(cityname)

    records = cursor.execute(f"SELECT * FROM {tableName} "
                   f"WHERE minLatitude <= {lat} "
                   f"AND maxLatitude >= {lat} "
                   f"AND minLongitude <= {lon} "
                   f"AND maxLongitude >= {lon} "
                   f"AND eventType = {eventType}")

    results = records.fetchall()

    cursor.close()
    connection.close()

    return resultDatabaseRecordsToDataframe(results)
def groupEventsByTime(df: pd.DataFrame) -> pd.DataFrame:
    """

    :param df: A dataframe of events from :func:`getAllRecordsForCity` or :func:`getAllRecordsForCityAndEventType`
    :return: A dataframe containing the event type, the start datetime and end datetime for all grouped events.
    """
    if df.empty:
        return df  # Return empty DataFrame if no data

    # Ensure events are sorted by eventTime
    df = df.sort_values(by=["eventType", "eventTime"])

    grouped_events = []

    for event_type, group in df.groupby("eventType"):
        group = group.sort_values(by="eventTime").reset_index(drop=True)

        # Determine the time step dynamically (either 1 hour or 1 day)
        if len(group) > 1:
            time_diffs = group["eventTime"].diff().dropna()
            if any(time_diffs == pd.Timedelta(hours=1)):
                time_delta = pd.Timedelta(hours=1)
            else:
                time_delta = pd.Timedelta(days=1)
        else:
            time_delta = pd.Timedelta(days=1)  # Default to daily if only one event

        current_start = group.iloc[0]["eventTime"]
        current_end = group.iloc[0]["eventTime"]

        for i in range(1, len(group)):
            row = group.iloc[i]
            if row["eventTime"] == current_end + time_delta:
                # Extend the current event range
                current_end = row["eventTime"]
            else:
                # Save the previous event range and start a new one
                grouped_events.append({
                    "eventType": event_type,
                    "startTime": current_start,
                    "endTime": current_end
                })
                current_start = row["eventTime"]
                current_end = row["eventTime"]

        # Append the last event
        grouped_events.append({
            "eventType": event_type,
            "startTime": current_start,
            "endTime": current_end
        })

    return pd.DataFrame(grouped_events)


