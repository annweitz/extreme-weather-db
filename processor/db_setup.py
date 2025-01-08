import sqlite3

# This script sets up the database schema for the processed data

# TODO: Adjust database schema according to requirements

# Setup database
DB_PATH = "processed_data.db"

# Initialise and connect to database
def initialize_database():
    """Creating database schema and tables for processed extreme weather events"""

    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Table for top 10 temperature events
    # id: unique identifier for each event
    # will populate depending on granularity of queries allowed for the user

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS temperature_top10_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        region TEXT,
        lat REAL,
        lon REAL,
        date TEXT,
        max_temp REAL,
        min_temp REAL,
        )
        """)
    
    # Table for heat spells
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS heat_spells (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   region TEXT,
                   lat REAL,
                   lon REAL,
                   start_date TEXT,
                   end_date TEXT,
                   duration INTEGER)""")
    
    # Table for top 10 wind events
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS wind_top10_events (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   region TEXT,
                   lat REAL,
                   lon REAL,
                   date TEXT,
                   max_wind_speed REAL)""")
    
    # Table for wind extremes
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS wind_extremes (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   region TEXT,
                   lat REAL,
                   lon REAL,
                   date TEXT,
                   max_wind REAL)""")
    
    # Table for precipitation events
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS precipitation_events (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   region TEXT,
                   lat REAL,
                   lon REAL,
                   date TEXT,
                   precipitation REAL)""")
    
    conn.commit()
    conn.close()

