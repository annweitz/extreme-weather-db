import glob
import os
import sys
import xarray




def main():
    year = sys.argv[1]
    variable = sys.argv[2]

    scratch_folder = "/scratch/ag-schultz/esdp2/"
    project_folder = "/projects/ag-schultz/"

    # save filenames for later deletion
    files = [x for x in glob.glob(scratch_folder + f"{variable}_{year}_*.nc")]

    # open datasets and merge them with xarray
    merged_dataset = xarray.open_mfdataset(files)

    # save merged dataset to netcdf file and close datasets
    merged_dataset.to_netcdf(f"{project_folder}{variable}_{year}.nc")
    merged_dataset.close()

    # remove now unneeded monthly files # dont do for now
    #for file in files:
    #    os.remove(file)

if __name__ == "__main__":
    main()