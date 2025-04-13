from calendar import isleap

def getDate(year, month):
    leapYear = isleap(year)
    month = int(month)
    if month in(1,3,5,7,8,10,12):
        day = 31
    elif month in (4,6,9,11):
        day = 30
    elif month == 2:
        if leapYear:
            day = 29
        else:
            day = 28
    if month < 10:
        month = f"0{month}"
    return f"{year}-{month}-01/to/{year}-{month}-{day}"




def getMinNC(dataset, variable):
    min = dataset[variable].min().to_numpy()
    return min

def getMaxNC(dataset, variable):
    max = dataset[variable].max().to_numpy()
    return max


def packDownloadRecords(records):
    arguments = []
    for row in records:
        retVal = f"{row[0]}:{row[1]}:{row[2]}"
        arguments.append(retVal)
    return arguments