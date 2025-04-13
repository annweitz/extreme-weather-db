

def initializeDatabase(connection, yearrange = [2000,2023], variables = ["temperature", "precipitation", "wind", "windgust"]):
    cursor = connection.cursor()

    # create table
    cursor.execute(
        "CREATE TABLE downloads(id INTEGER PRIMARY KEY, year INTEGER, month INTEGER, variable TEXT, status TEXT, tries INTEGER)")

    # fill table
    for year in range(yearrange[0], yearrange[1] + 1):
        for month in range(1, 13):
            for var in variables:
                cursor.execute(f"""
                INSERT INTO downloads (id, year, month, variable, status, tries) VALUES
                (NULL, {year},{month}, '{var}' ,'unknown', 0)
                """)

    # commit results
    connection.commit()
    return

def updateStatus(year,month,var,status,cursor):
    cursor.execute(f"UPDATE downloads SET status = '{status}' WHERE year={year} AND month = {month} AND variable = '{var}'")
    con = cursor.connection
    con.commit()

def incrementTries(year, month, var, cursor):
    cursor.execute(f"UPDATE downloads SET tries = tries + 1  WHERE year={year} AND month = {month} AND variable = '{var}'")
    con = cursor.connection
    con.commit()

def getTriesByID(id, cursor):
    result = cursor.execute(f"SELECT tries FROM downloads WHERE id = {id}")
    tries = result.fetchone()[0]
    return tries

def getTries(year,month,var,cursor):
    result = cursor.execute(f"SELECT tries FROM downloads WHERE year = {year} AND month = {month} AND variable = '{var}'")
    tries = result.fetchone()[0]
    return tries

def getStatusByID(id, cursor):
    result = cursor.execute(f"SELECT status FROM downloads WHERE id = {id}")
    tries = result.fetchone()[0]
    return tries

def getStatus(year,month,var,cursor):
    result = cursor.execute(f"SELECT status FROM downloads WHERE year = {year} AND month = {month} AND variable = '{var}'")
    status = result.fetchone()[0]
    return status