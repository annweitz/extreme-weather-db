import os
import sqlite3
import xarray as xr
import db_setup as db

"""
Functions to
- Load data from raw database
- Process data differently according to which variable it belongs to
- Store processed data in querying database
"""

# TODO: Check name of variables in the raw data files

db.initialise_database()

RAW_DATA_DIR = "/scratch/ag-schultz/esdp2/"  
QUERY_DB_PATH = "/projects/ag-schultz/processed_querying.db"

# Feed-in data from raw database. Specific data loading for each file to allow parallel processing.
def load_data(variable=None, year=None, month=None):
    """Load data from raw database. Process data differently according to which variable it belongs to.
    Args:
        variable: str, variable to be processed
        year: int, year to be processed
        month: int, month to be processed
    Returns: Data for a specific variable, year and month
    """
    filepath = RAW_DATA_DIR + f'variable{year}_{month}.nc'
    if not os.path.exists(filepath):
        print(f"File {filepath} does not exist.")
        return None
    # Load data from raw database
    data = xr.open_dataset(filepath)
    return variable, data

def process_data(variable, data):
    """Process data differently according to which variable it belongs to. 
    Args:
        variable: str, variable to be processed
        data: xarray.Dataset, data to be processed
    Returns: None
    """
    if variable == "temperature":
        process_temperature_data(data)
    elif variable == "wind":
        process_wind_data(data)
    elif variable == "precipitation":
        process_precipitation_data(data)
    else:
        print(f"Variable {variable} not recognized.")


def process_temperature_data():
    pass

def process_precipitation_data():
    pass

def process_wind_data(ds, year, month, cursor):
    """
    Process wind data to detect extreme events (Beaufort 10) and store in database.

    :param ds: xarray Dataset of wind data
    :param year: Year of the data
    :param month: Month of the data
    :param cursor: SQLite cursor for database operations
    """
    # Calculate wind speed from U and V components
    wind_speed = (ds['10m_u_component_of_wind']**2 + ds['10m_v_component_of_wind']**2)**0.5

    # Beaufort 10 threshold
    beaufort_10_threshold = 20.8  # m/s
    extreme_events = wind_speed >= beaufort_10_threshold

    # Extract timestamps of extreme events
    extreme_times = ds.time[extreme_events.any(dim=['latitude', 'longitude'])].values

    # Insert extreme events into the database
    for timestamp in extreme_times:
        cursor.execute("""
            INSERT INTO wind_extremes (year, month, timestamp, event_type) 
            VALUES (?, ?, ?, ?)
        """, (year, month, timestamp, "Beaufort 10"))

    print(f"Processed and stored wind extreme events for {year}-{month:02d}.")
    return