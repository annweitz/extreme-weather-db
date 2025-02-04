'''
Script to extract data from the netCDF files and process it to create a database of extreme events based on a threshold value.
The netCDF files are in the ERA5 format and contain weather data for a specific year.
The variables to be processed are temperature, wind, precipitation and windgust (instantaneous wind at 10m height).
The threshold values were chosen based on definitions from weather services and literature.
Extreme wind events are used to identify storm events.
'''

import sqlite3 
import os # library to interact with the OS
import xarray as xr
import numpy as np
import glob
import logging
from scipy.ndimage import label # used to identify clusters of storm events
from concurrent.futures import ProcessPoolExecutor # used to parallelize processing
from skimage.measure import regionprops, find_boundaries # used to calculate storm properties

# set up logging
logging.basicConfig(filename='threshold_processing.log',level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')

# Define paths to the download database and new processing database
download_db_path = "/projects/ag-schultz/download_database.db"
processing_db_path = "/projects/ag-schultz/processed_database.db"

class DataProcessor:
    """
    Base class for all data processors. Processes one year at a time.  
    """
    def __init__(self, dataset, year):
        self.dataset = dataset
        self.year = year
        
    def process(self):
        """
        Process the data to find extreme events.
        """
        raise NotImplementedError("Subclasses must implement this method.")
    
class WindProcessor(DataProcessor):
    """
    Processes daily wind speed values based on the vector sum of u10 and v10 components.
    Masks wind speeds that exceed extreme thresholds (Beaufort 9).
    """

    def process(self):
        extreme_winds = [] # list to store extreme wind events
        try: 
            u10 = self.dataset["u10"].values  # Extract u-component of wind
            v10 = self.dataset["v10"].values  # Extract v-component of wind
            wind_speed = np.sqrt(u10**2 + v10**2)  # Compute vector sum (wind speed)
            extreme_mask = wind_speed >= 24.5  # Mask extreme wind events (Beaufort 9)

            for day in range(self.dataset.dims['valid_time']): # iterate over days in the dataset
                daily_mask = extreme_mask[day] # get the mask for the current day
                extreme_winds.append(self.year, day, daily_mask)
        except Exception as e:
            logging.error(f"Error processing wind data for year {self.year}: {e}")

        return extreme_winds
    
class StormProcessor(DataProcessor):
    """
    Detects and charcterises storm events based on extreme wind events.
    Groups neighbouring pixels into storm clusters, then determines extent, shape and duration of each storm.
    Classifies storms using metteorological characteristics from literature:
    - Tropical Cyclones: Circular shape, wind speed > 33 m/s, spatial extent > 800 km in x or y direction
    - Extratropical Cyclones: Frontal systems, comma- or irregular-shaped, spatial extent > 800 km in x or y direction
    - Convective storms: Highly circular, spatial extent 200-800 km in x or y direction
    - 
    """
    def process(self,wind_data):
        storms = [] # list to store storm events
        for storm in wind_data:
            year, day, daily_mask = storm
            labeled_array, num_features = label(daily_mask) # Label connected components in the mask

            for region in regionprops(labeled_array,intensity_image=daily_mask):

            for storm_id in range(1, num_features + 1):
                indices = np.argwhere(labeled_array == storm_id) # Get indices of pixels in the storm cluster
                lat_values = [grid_cells[idx[0]][0] for idx in indices] # Extract latitude values
                lon_values = [grid_cells[idx[0]][1] for idx in indices] # Extract longitude values

                # Calculate spatial extent and aspect ratio of the storm cluster
                min_lat, max_lat = min(lat_values), max(lat_values) # Calculate min and max latitude
                min_lon, max_lon = min(lon_values), max(lon_values) # Calculate min and max longitude
                extent_x = (max_lon - min_lon) * 0.25 # Calculate extent in x-direction
                extent_y = (max_lat - min_lat) * 0.25 # Calculate extent in y-direction
                aspect_ratio = extent_x / (extent_y + 1e-9) # Prevent division by 0

                # Shape classification
                circularity 
                shape_type = 
                