import xarray as xr

project_folder = "/projects/ag-schultz/"
def changeYear(filepath, year):

    previousFile = filepath.replace(str(year), str(year-1))

    merged = xr.open_mfdataset([previousFile, filepath])

    sliceBeginning = f"{year}-01-01 00:00:00"
    sliceEnd = f"{year}-12-31 23:00:00"

    mergeSlice = merged.sel(valid_time = slice(sliceBeginning, sliceEnd))

    new_filepath = filepath.replace("precipitation","precipitation_new")

    mergeSlice.to_netcdf(new_filepath)
    merged.close()
    mergeSlice.close()

def main():

    years = range(2000, 2024)

    for year in years:
        filepath = project_folder+"precipitation_" + str(year) + ".nc"
        changeYear(filepath, year)
        print(f"finished year {year}")

    print("finished all years")

    pass

if __name__ == main():
    main()