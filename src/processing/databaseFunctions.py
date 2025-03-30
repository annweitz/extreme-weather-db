import glob
import os.path
import sqlite3
import os
import pandas as pd
from src.config import PROCESSING_FOLDER, PROCESSING_DATABASE, RESULT_DATABASE

def splitFilename(filename):
    try:
        var, year = filename.split("_", maxsplit = 1)
    except ValueError:
        return (None, None)
    year = year[0:-3]

    # In case of single months being in the same directory
    if(len(year)>4):
        return (None, None)
    else:
        return (var, year)


def createProcessingDatabase(pathToFiles = PROCESSING_FOLDER, pathToProcessingDB = PROCESSING_DATABASE, tablename = "processing"):
    """
    Creates a table for storing the processing status of all .nc files contained in pathToFiles.
    :param pathToFiles: Path to files to process. Default: PROCESSING_FOLDER path specified in config
    :param pathToProcessingDB: Path to the processing database. Default: PROCESSING_DATABASE path specified in config
    :param tablename: Name for the processing table. Default: processing
    """
    # establish sql connection to database
    connection = sqlite3.connect(pathToProcessingDB)
    cursor = connection.cursor()

    # Create table
    cursor.execute(f"CREATE TABLE {tablename} (id INTEGER PRIMARY KEY, variable TEXT, year INTEGER, status TEXT)")

    # get all .nc files in directory
    files = [x for x in glob.glob(f"{pathToFiles}*.nc")]

    # read filenames into data
    data = []
    for file in files:
        # remove path from the file
        file = os.path.basename(file)
        x = splitFilename(file)
        if(x == (None,None)):
            continue
        else:
            data.append(x)

    # Insert all results into the table
    connection.executemany(f"INSERT INTO {tablename} (variable, year,  status) VALUES (?,?,'unprocessed')", data)
    connection.commit()

    # Close the connection
    cursor.close()
    connection.close()


def updateProcessingDatabase(pathToFiles = PROCESSING_FOLDER, pathToProcessingDB = PROCESSING_DATABASE, tablename="processing"):
    """

    :param pathToFiles: Path to files to process. Default: PROCESSING_FOLDER path specified in config
    :param pathToProcessingDB: Path to the processing database. Default: PROCESSING_DATABASE path specified in config
    :param tablename: Name for the processing table. Default: processing
    :return: The number of new records created in the database
    """

    # Establish SQL connection to the database
    connection = sqlite3.connect(pathToProcessingDB)
    cursor = connection.cursor()

    # Get all .nc files in the directory
    files = [x for x in glob.glob(f"{pathToFiles}*.nc")]

    # Read filenames into data
    data = []
    for file in files:
        file = os.path.basename(file)
        x = splitFilename(file)  # Assuming this function extracts (variable, year)
        if x == (None, None):
            continue

        variable, year = x

        # Check if the record already exists
        cursor.execute(f"SELECT COUNT(*) FROM {tablename} WHERE variable = ? AND year = ?", (variable, year))
        exists = cursor.fetchone()[0]

        if exists == 0:
            data.append((variable, year))

    # Insert new records
    if data:
        cursor.executemany(f"INSERT INTO {tablename} (variable, year, status) VALUES (?, ?, 'unprocessed')", data)
        connection.commit()
        numNewRecords = len(data)
    else:
        numNewRecords = 0

    # Close the connection
    cursor.close()
    connection.close()

    return numNewRecords

def updateProcessingStatus(pathToProcessingDB, year, var, status):
    connection = sqlite3.connect(pathToProcessingDB)
    cursor = connection.cursor()

    cursor.execute(f"UPDATE processing SET status = '{status}' WHERE year={year} AND variable = '{var}'")
    connection.commit()

    cursor.close()
    connection.close()

def createResultDatabase(pathToResultDB, tableName = "thresholdResults"):
    # establish sql connection to database
    connection = sqlite3.connect(pathToResultDB)
    cursor = connection.cursor()

    exists = cursor.execute(f"Select exists(select 1 from sqlite_master where type = 'table' and name = '{tableName}')").fetchone()[0]
    if exists == 0:
        # Create table
        cursor.execute(f"CREATE TABLE {tableName} (id INTEGER PRIMARY KEY, "
                       f"eventType TEXT, "
                       f"eventTime DATE, "
                       f"minLatitude FLOAT, "
                       f"maxLatitude FLOAT, "
                       f"minLongitude FLOAT, "
                       f"maxLongitude FLOAT, "
                       f"centroidLatitude FLOAT, "
                       f"centroidLongitude FLOAT, "
                       f"maxEventValue FLOAT, "
                       f"meanEventValue FLOAT, "
                       f"eventArea INT)")

        connection.commit()

    # Close the connection
    cursor.close()
    connection.close()


def insertEventsIntoDatabase(pathToResultDB, eventType,events, tableName = "thresholdResults"):
    # establish sql connection to database
    connection = sqlite3.connect(pathToResultDB)
    cursor = connection.cursor()

    # SQL statement to insert event data
    insert_query = f"""
    INSERT INTO {tableName} (
        eventType, eventTime, minLatitude, maxLatitude, minLongitude, maxLongitude,
        centroidLatitude, centroidLongitude, maxEventValue, meanEventValue, eventArea
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    # Prepare the data for insertion
    event_data = [
        (
            eventType, event['eventTime'], event['minLatitude'], event['maxLatitude'],
            event['minLongitude'], event['maxLongitude'], event['centroidLatitude'],
            event['centroidLongitude'], event['maxEventValue'], event['meanEventValue'],
            event['areaInCells']
        )
        for event in events
    ]

    # Execute the batch insert
    connection.executemany(insert_query, event_data)
    connection.commit()
    # Close the connection
    cursor.close()
    connection.close()


def resultDatabaseRecordsToDataframe(records):
    """
    Converts records from the result database into a dataframe. Also changes eventTime to datetime format.
    :param records: Records from the result database
    :return: A dataframe containing the records.
    """
    df =  pd.DataFrame(records, columns=["id", "eventType", "eventTime", "minLatitude", "maxLatitude", "minLongitude", "maxLongitude",
                                        "centroidLatitude", "centroidLongitude", "maxEventValue", "meanEventValue", "eventArea"])
    df["eventTime"] = pd.to_datetime(df["eventTime"])
    return df
