'''
Script to extract data from the netCDF files and process it to create a database of extreme events based on a threshold value.
The netCDF file names are listed in the 'downloads' table of the download_database.db database and located in the same folder.
The variables to be processed are temperature, wind, precipitation and windgust (instantaneous wind at 10m height).
The threshold values were chosen based on definitions from weather services and literature.
'''

import sqlite3
import os
import xarray as xr
import numpy as np
import glob

# Define paths to the download database and new processing database
download_db_path = "/projects/ag-schultz/download_database.db"
processing_db_path = "/projects/ag-schultz/processed_database.db"

# Define the threshold values for extreme weather events
thresholds = {
    "temperature": 30,  # degrees Celsius
    "wind": 15,  # m/s
    "precipitation": 10,  # mm/h
    "windgust": 20  # m/s
}

class ThresholdProcessor:
    """Class to process threshold values for extreme weather events.
    Attributes:
    variable (str): The variable to process (e.g., temperature, wind, precipitation, windgust).
    year (int): The year to process the data for.
    """

    def __init__(self,variable,year):
        self.variable = variable
        self.year = year
    
    def process(self):
        raise NotImplementedError("Subclass must implement process method")
    
class TemperatureProcessor(ThresholdProcessor):
    """Process temperature data which contains daily mean temperature values."""

    def process(self):
        