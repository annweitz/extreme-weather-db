import sqlite3

# Connect to the database
conn = sqlite3.connect("/projects/ag-schultz/download_database.db")
cursor = conn.cursor()

# Define the range of years and the variable name
start_year = 2000
end_year = 2023
variable = "windgust"

# Create a list of data for all months and years
data = [
    (None, year, month, variable, "pending", 0)
    for year in range(start_year, end_year + 1)
    for month in range(1, 13)
]

# Insert rows into the 'downloads' table
cursor.executemany("INSERT INTO downloads VALUES (?, ?, ?, ?, ?, ?)", data)

conn.commit()
conn.close()

print(f"Rows for {start_year} to {end_year} with variable '{variable}' added successfully.")
