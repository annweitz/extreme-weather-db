import xarray as xr 
import sys
import os
import numpy as np
import glob
import sqlite3
import logging
from scipy.ndimage import label

def inspect_dataset(file_path):
    """
    Inspect the contents of a netCDF dataset
    """
    try: 
        # open netcdf file as xarray dataset
        ds = xr.open_dataset(file_path)

        print(f"Inspecting dataset: {file_path}")

        # print available dimensions
        print("Dimensions:")
        for dim, size in ds.dims.items(): # items() returns key-value pairs of the dictionary returned by ds.dims
            print(f" - {dim}: {size}")
            # example output: - time: 365 

        # print available variables (columns) in the dataset
        print("Variables:")
        for var in ds.variables.keys(): # keys() returns the keys of the dictionary returned by ds.variables
            print(f" - {var} (Data Type: {ds[var].dtype})")
            # example output: u10 (Data Type: float32)
        
        # print global attributes ie. metadata of the dataset
        print("Global Attributes:")
        for attr in ds.attrs.items(): # items() returns key-value pairs of the dictionary returned by ds.attrs
            print(f" - {attr[0]}: {attr[1]}")
            # example output: - title: ERA5 hourly data on single levels from 1979 to present

        # close dataset
        ds.close()

    except Exception as e:
        print(f"Error: {e}")

    # run script from command line
if __name__ == "__main__":
    """
    To make script executable from command line
    Example usage: python inspect_dataset.py /path/to/dataset.nc
    """
    if len(sys.argv) != 2:
        print("Usage: inspect_dataset.py <file_path>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        sys.exit(1)
    
    inspect_dataset(file_path)
