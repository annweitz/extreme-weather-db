import cdsapi
import os
import sqlite3
import xarray as xr
import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Use DEBUG level for detailed logs
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Directories
scratch_dir = "/scratch/akhan10"  # Temporary storage on scratch
raw_data_dir = f"{scratch_dir}/raw_data"  # Raw downloads
processed_data_dir = f"{scratch_dir}/processed_data"  # Processed files in scratch
final_storage_dir = "/home/akhan10/processed_data"  # Permanent storage in home

# SQLite Database
db_path = "/home/akhan10/data_management.db"

# Variables to download
variables = [
    '10m_u_component_of_wind',
    '10m_v_component_of_wind',
    'instantaneous_10m_wind_gust',
]

# Wind threshold for sustained wind
sustained_wind_threshold = 10  # m/s

# Years and months to download
years = range(1960)  # Example: Adjust as needed
months = [f"{i:02}" for i in range(1, 2)]  # Months 01 to 12


def setup_directories():
    """Ensure all necessary directories exist."""
    try:
        os.makedirs(raw_data_dir, exist_ok=True)
        os.makedirs(processed_data_dir, exist_ok=True)
        os.makedirs(final_storage_dir, exist_ok=True)
        logging.info("Directories set up successfully.")
    except Exception as e:
        logging.error(f"Failed to set up directories: {e}")


def setup_database():
    """Initialize the SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER,
                month INTEGER,
                file_path TEXT,
                validated BOOLEAN DEFAULT 0,
                validation_log TEXT,
                UNIQUE(year, month)
            )
        """)
        conn.commit()
        conn.close()
        logging.info("Database setup successfully.")
    except Exception as e:
        logging.error(f"Failed to set up database: {e}")


def insert_metadata(year, month, file_path):
    """Insert metadata into the database."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO downloads (year, month, file_path)
            VALUES (?, ?, ?)
        """, (year, month, file_path))
        conn.commit()
        conn.close()
        logging.info(f"Metadata inserted for {year}-{month}.")
    except Exception as e:
        logging.error(f"Failed to insert metadata for {year}-{month}: {e}")


def update_validation_status(year, month, status, log=""):
    """Update validation status in the database."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE downloads
            SET validated = ?, validation_log = ?
            WHERE year = ? AND month = ?
        """, (status, log, year, month))
        conn.commit()
        conn.close()
        logging.info(f"Validation status updated for {year}-{month}.")
    except Exception as e:
        logging.error(f"Failed to update validation status for {year}-{month}: {e}")


def validate_file(file_path, year, month):
    """Validate downloaded NetCDF file."""
    try:
        ds = xr.open_dataset(file_path)
        # Check for required variables
        for var in variables:
            if var not in ds.variables:
                raise ValueError(f"Missing variable: {var}")
        logging.info(f"Validation passed for {file_path}.")
        update_validation_status(year, month, True)
    except Exception as e:
        logging.error(f"Validation failed for {file_path}: {e}")
        update_validation_status(year, month, False, str(e))


def process_and_transfer(file_path, year, month):
    """Process data and transfer validated files to home directory."""
    try:
        logging.info(f"Processing {file_path}...")
        ds = xr.open_dataset(file_path)

        # Calculate wind speed
        wind_speed = (ds['10m_u_component_of_wind']**2 + ds['10m_v_component_of_wind']**2)**0.5

        # Identify sustained wind above threshold
        sustained_wind = wind_speed >= sustained_wind_threshold
        ds['sustained_wind'] = (('time', 'latitude', 'longitude'), sustained_wind)

        # Save processed data temporarily as NetCDF
        output_file = f"{processed_data_dir}/era5_{year}_{month}.nc"
        ds.to_netcdf(output_file)

        # Transfer processed data to home directory
        final_file_path = f"{final_storage_dir}/era5_{year}_{month}.nc"
        os.rename(output_file, final_file_path)
        logging.info(f"Processed data moved to {final_file_path}.")
    except Exception as e:
        logging.error(f"Failed to process and transfer {file_path}: {e}")


def download_data(year, month, raw_data_dir, variables):
    """
    Download ERA5 daily statistics for a specific year and month.
    """
    output_file = f"{raw_data_dir}/era5_{year}_{month}.nc"

    if not os.path.exists(output_file):
        logging.info(f"Downloading data for {year}-{month}...")
        try:
            c = cdsapi.Client()
            logging.debug("Submitting request to CDS...")
            c.retrieve(
                'derived-era5-single-levels-daily-statistics',
                {
                    'product_type': 'reanalysis',
                    'variable': variables,
                    'year': str(year),
                    'month': [str(month)],
                    'day': [f"{i:02}" for i in range(1, 32)],
                    'daily_statistic': 'daily_maximum',
                    'time_zone': 'utc+00:00',
                    'frequency': '1_hourly'
                },
                output_file,
            )
            logging.info(f"Downloaded successfully: {output_file}.")
        except Exception as e:
            logging.error(f"Error downloading {year}-{month}: {e}")
            return None
    else:
        logging.info(f"File already exists: {output_file}.")

    return output_file


def main():
    """Main function for downloading, validating, and processing data."""
    try:
        setup_directories()
        setup_database()

        for year in years:
            for month in months:
                # Ensure correct arguments are passed
                file_path = download_data(year, month, raw_data_dir, variables)
                if file_path:  # Proceed only if file is downloaded successfully
                    validate_file(file_path, year, month)
                    process_and_transfer(file_path, year, month)
    except Exception as e:
        logging.critical(f"Unexpected error in main execution: {e}")


if __name__ == "__main__":
    main()
