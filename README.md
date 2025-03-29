# extreme-weather-db

An extreme weather database project developed during the **Advanced Earth System Data Processing** course at the **University of Cologne** in the Winter Semester of 2024/2025, under the guidance of [**Prof. Dr. Martin Schultz**](https://go.fzj.de/martinschultz).

---

### 📌 About the Project

This project processes and analyzes climate data to detect **extreme weather events** that occurred between **2000 and 2023**. It utilizes datasets obtained from the **ERA5 reanalysis archive** provided by the Copernicus Climate Data Store (CDS).

The core variables include:
- **Temperature** at 2 meters
- **Precipitation**
- **Wind** (u and v components at 10 meters)
- **Wind gusts** (instantaneous at 10 meters)

---

### 🔍 How It Works

The dataset is analyzed and stored in a **SQLite database**, supporting two primary query types:

1. **Top-N Queries**: Retrieve the top 10 highest (or lowest) values for a selected variable in a user-defined time window and region.
2. **Threshold-Based Queries**: Detect physical quantities that exceed thresholds defined in meteorological literature — for example:
   - Extremely high temperatures that pose risks to human life
   - Intense rainfall or snow events
   - Storm-strength wind and gust events

---

### 📊 Statistical Aggregation

- **Temperature & Precipitation**: Daily **minimum** and **maximum**
- **Wind & Wind Gusts**: Daily **maximum**
- **Connected Event Detection**: Events are clustered based on spatial and temporal continuity

---

### 🗺️ Spatial & Temporal Resolution

- **Grid resolution**: 0.25° x 0.25° (~25 km)
- **Temporal resolution**:
  - Hourly: Temperature, wind, wind gusts
  - Daily: Precipitation

---

### ⚙️ Data Access & Setup

To use the download functionality:
- Create a **CDS account** and agree to the license terms
- Set up your `.cdsapirc` file with your CDS API key as per the [CDS API instructions](https://cds.climate.copernicus.eu/api-how-to)
- Define your dataset selections and variable requests in the script

The code is designed to be **extensible**, using a **factory pattern** to manage different variables, and includes functionality for **automated download tracking**, **sanity checking**, and **result storage**.

---

#  Table of contents

1. about
2. Table of contents
3. Requirements
4.. Installation 
    1.download
    2.Installing dependencies and setting up virtual environment with poetry
5. Examples
    1. Retrieving data
    2. Top 10 query
    3. Threshold processing query
    4. 
6. How the tool works
    1. downloading
    4. Processing

7. Acknowledgements
8. Documentation of source code
9. Citation

# requirements

This project requires python 3.11 or higher. For more information see pyproject.toml
The package relies on netCDF and X arrays for saving data, and if they are required as dependencies in the installation

 Thread pool executor is used for parallel processing on a single node and a slum script is used for running the scripts on the hpc called Ramses.

The examples in this package rely on 

#  Installation

 In the folder that you want to download this project to clone the project from its get repository

Git clone https://github.com/annweitz/extreme-weather-db

 The dependency management in this project is done by poetry so we need to instal that first using the command pip instal poetry

 Poetry then instals the dependencies and creates a virtual environment using poetry init  And poetry instal 

 To deactivate the environment we need to run

 To run the example notebooks

 To run the scripts we need to call poetry run python path to script

# Examples 

#  How the tool works

##  downloading

### ERA5 Climate Data Downloader (`downloader.py`)

This module automates the downloading, sanity checking, and merging of ERA5 climate data from the [Copernicus Climate Data Store (CDS)](https://cds.climate.copernicus.eu/). It supports wind gusts, wind vectors, temperature at 1000 hPa, and total precipitation over the period from 2000 to 2023.

Key features:
- Modular request builders using the **factory pattern** for each variable
- Multi-threaded downloading with `ThreadPoolExecutor` (default: 8 workers)
- Sanity checks for file size and variable value ranges
- Download status tracking using a **SQLite database**
- Automatic merging of monthly files into yearly NetCDF files
- Uses `/scratch/ag-schultz/esdp2/` as the temporary storage path and `/projects/ag-schultz/` as the final output path

#### How it works
1. The database `download_database.db` is initialized (if not already present) with download tasks for each month, year, and variable.
2. The script queries all tasks not yet marked as downloaded and attempts to download them using the CDS API.
3. Each file is validated based on:
   - File size range (specific to variable)
   - Value range (e.g., wind gust between 0 and 150 m/s)
4. Validated downloads are marked as `"downloaded"` in the database. If a full year (12 months) is completed for a variable, the script calls `merge_script.py` to combine the files.
5. Each failed download is retried up to 15 times (configurable).

#### Parallel Execution
- Runs on a **single node** using multithreading
- For HPC environments like **RAMSES**, the script can be submitted with SLURM using `--cpus-per-task` matching the `max_workers`

To run:

```bash
poetry run python downloader.py

Ensure that you have:

Your .cdsapirc credentials set up in your home directory

Access to the appropriate storage folders (scratch and projects)

## processing


This script processes the downloaded NetCDF data to extract **extreme weather events**, update the **top 10 events**, and insert detected events into a SQLite **results database**.

#### Key Components Used:
- `sqlite3`- `xarray`: - `ThreadPoolExecutor`: 
- Custom modules:
  - `databaseFunctions`: For DB creation and event storage
  - `processing_functions`: For connected-event detection and top-10 calculations

#### Hardcoded Constants
- `PROCESSING_FOLDER`, `RESULT_FOLDER`: define working directories
- `processing.sql` and `results.sql`: databases for tracking status and storing events
- `MAX_WORKERS`: thread pool size (currently 1)

#### 🏭 ProcessingFactory
A modular design using the **Factory Pattern** to register and call the appropriate processing logic for each variable:
```python
ProcessingFactory.registerProcessor("precipitation", lambda path: processPrecipitation(path))

#### `processPrecipitation(path)`

This function handles **threshold-based** and **top-N precipitation event detection** using hourly and daily coarsened datasets.

##### 1. Splitting Precipitation Types
- Uses `ptype` variable to distinguish between:
  - **Rain types**: rain, freezing rain, rain-snow mix, freezing drizzle
  - **Snow types**: snow, wet snow, ice pellets
- Creates two hourly datasets:
  - `rainHourlyDS` – only rain-type precipitation
  - `snowHourlyDS` – only snow-type precipitation

##### 2. Hourly Threshold Events
- Rain events where **hourly precipitation > 0.1 m** are extracted.
- Spatially and temporally connected exceedances are grouped using `getConnectedEvents()`.
- Events are inserted into the **`rainHourly`** table in the results database.

##### 3. 🏆 Daily Top 10 Events

- Coarsens full precipitation data to **daily totals**.
- Compares with existing `top10precipitation.nc` file (if present).
- Updates and saves new top 10 daily extreme precipitation values.

##### 4. Daily Threshold Events
- Rain and snow hourly datasets are coarsened to **daily totals**.
- Thresholds used:
  - Rain: **> 0.2 m/day**
  - Snow: **> 0.1 m/day**
- Connected extreme events are clustered and inserted into:
  - `rainDaily`
  - `snowDaily`

### 🌡️ `processTemperature(path)`

Processes temperature data at the 1000 hPa pressure level.

#### 🔧 Processing Steps:
- Selects only the first pressure level and removes redundant coordinates (`pressure_level`, `number`).
- Computes and updates:
  - Top 10 **highest** daily temperatures (saved to `top10temperatureHigh.nc`)
  - Top 10 **lowest** daily temperatures (saved to `top10temperatureLow.nc`)
- Existing top 10 datasets are loaded and compared using `update_top_n()` to preserve consistency across years.

#### 🗃️ Output:
- Top 10 files in NetCDF format

### 💨 `processWind(path)`

Processes 10m wind data (u and v components) to detect high wind events.

#### 🔧 Processing Steps:
- Computes the windspeed magnitude:  
  \[`windspeed = sqrt(u10² + v10²)`\]
- Drops unnecessary fields (`u10`, `v10`, `i10fg`, `number`) for efficiency.
- Detects:
  - Top 10 highest windspeed days (`top10wind.nc`)
  - Connected windspeed events exceeding **Beaufort 9 (20.8 m/s)**
- Events are clustered using `getConnectedEvents()` and stored in the `windspeedDaily` table.

#### 🗃️ Output:
- `top10wind.nc` NetCDF file
- `windspeedDaily` events stored in the result database

---
### 🌬️ `processWindgust(path)`

Processes hourly instantaneous 10m wind gust data to detect extreme gust events.

#### 🔧 Processing Steps:
- Removes unused dimensions (`number`, `expver`)
- Detects wind gust exceedances over **Beaufort 10 (24.5 m/s)**
- Clusters and inserts connected gust events into the `windgustHourly` table

> 💡 **Note**: Top 10 detection is commented out in this version and can be enabled later.

#### 🗃️ Output:
- `windgustHourly` events stored in the results database

---

### 🧠 `processingManager(arguments)`

Main function executed per dataset (per variable and year).

#### 🔧 Functionality:
- Splits input into `year` and `variable`
- Fetches appropriate processor from `ProcessingFactory`
- Updates processing status to `"processing"` in the SQLite tracking DB
- Calls the processing function
- Updates processing status to `"processed"` after completion
- Logs all key steps and errors

---

### 🔁 `main()` — Entry Point

Handles full execution of the processing pipeline.

#### 🔧 Workflow:
1. Connects to the processing database (`processing.sql`)
2. Initializes the DB schema if it doesn't exist
3. Fetches all years/variables that haven’t been processed
4. Packs arguments as `"year:variable"` format
5. Creates the results database (`results.sql`)
6. Processes all records in parallel using `ThreadPoolExecutor`

#### 🧩 Parallelism:
- Controlled by `MAX_WORKERS` (currently set to 1)
- Easily scalable for local HPC or cloud environments

#### 🗃️ Output:
- Logs written to `processing.log`
- All extreme event detections stored in:
  - Result NetCDF files
  - SQLite event database



