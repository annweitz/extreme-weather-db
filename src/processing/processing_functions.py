import numpy as np
import xarray as xr
from scipy.ndimage import label
from xarray import apply_ufunc

def update_top_n(dataset, data_var, time_var="valid_time", oldTop10=None, highest = True, topN=10, ):
    """
    Given a dataset and the corresponding data variable data_var, creates or updates a top N of
    highest or lowest values using vectorized operations. Output is a dataset containing the values
    and their corresponding times.


    Parameters
    ----------
    dataset : xarray.Dataset
        Dataset containing:
          - A data variable (specified by data_var) that should only contain the dimensions (time, latitude, longitude)
          - A time variable (specified by time_var) which may be one-dimensional (time only) or already have spatial dimensions
    data_var : str,
        Name of the variable in the dataset for which to compute the top n values.
    time_var : str, default "valid_time"
        Name of the variable representing the time corresponding to data_var.
    oldTop10 : xarray.Dataset, optional
        Dataset with the old top 10 data, containing two DataArrays:
          - "top_{data_var}" with dims ('rank', 'latitude', 'longitude')
          - "top_{time_var}" with dims ('rank', 'latitude', 'longitude')
        If None, a new template filled with NaN is created.
    highest : bool, default True
        If true, returns the 10 highest values, if False, returns the 10 lowest values
    topN : int, default 10
        Number of top values to track.

    Returns
    -------
    xr.Dataset
        A Dataset containing:
          - "top_{data_var}": top values of the specified variable (dims: 'rank', 'latitude', 'longitude')
          - "top_{time_var}": corresponding time values (dims: 'rank', 'latitude', 'longitude')
    """

    # Determine grid dimensions.
    nlat = dataset.latitude.size
    nlon = dataset.longitude.size
    ranks = np.arange(1, topN + 1)

    # If no old top 10 is provided, create a template with two variables
    if oldTop10 is None:
        top_data = xr.DataArray(
            np.full((topN, nlat, nlon), np.nan),
            coords={'rank': ranks, 'latitude': dataset.latitude, 'longitude': dataset.longitude},
            dims=['rank', 'latitude', 'longitude']
        )
        top_time = xr.DataArray(
            np.full((topN, nlat, nlon), np.datetime64("NaT", "ns")),
            coords={'rank': ranks, 'latitude': dataset.latitude, 'longitude': dataset.longitude},
            dims=['rank', 'latitude', 'longitude']
        )
        oldTop10 = xr.Dataset({
            f"top_{data_var}": top_data,
            f"top_{time_var}": top_time
        })

    # Stack the spatial dimensions into a single "point" dimension for the data variable
    stacked_data = dataset[data_var].stack(point=['latitude', 'longitude'])

    # Determine the time dimension name (assumes dims other than 'latitude' and 'longitude').
    time_dim = [dim for dim in dataset[data_var].dims if dim not in ['latitude', 'longitude']][0]

    # Prepare the time variable so that it has the same (time, point) shape as the data.
    # If the time variable already contains spatial dims, we broadcast and stack it.
    # Otherwise (the default case where the time variable is 1-dimensional), we repeat its values across all points.
    if set(['latitude', 'longitude']).issubset(dataset[time_var].dims):
        stacked_times = dataset[time_var].broadcast_like(dataset[data_var]).stack(point=['latitude', 'longitude'])
    else:
        n_points = stacked_data.shape[1]
        # Ensure the time variable is 1-D.
        time_vals = dataset[time_var].values.reshape(-1)
        # Use np.repeat to create an array of shape (T, n_points)
        tiled_times = np.repeat(time_vals[:, None], n_points, axis=1)
        stacked_times = xr.DataArray(
            tiled_times,
            dims=(time_dim, 'point'),
            coords={time_dim: dataset[time_var].values, 'point': stacked_data.point.values}
        )

    # Convert the stacked DataArrays to NumPy arrays.
    data_values = stacked_data.values    # shape: (n_time, n_points)
    time_values = stacked_times.values    # shape: (n_time, n_points)

    # Retrieve current top 10 values and times (both stacked to (n_rank, n_points)).
    current_values = oldTop10[f"top_{data_var}"].stack(point=['latitude', 'longitude']).values
    current_times  = oldTop10[f"top_{time_var}"].stack(point=['latitude', 'longitude']).values

    # Combine the current top values with the new data along the time axis
    all_values = np.concatenate([current_values, data_values], axis=0)
    all_times  = np.concatenate([current_times, time_values], axis=0)

    # Replace NaNs with -infinity or infinity so they are never selected as top values, depending on if high or low values are wanted
    if(highest):
        filled_values = np.where(np.isnan(all_values), -np.inf, all_values)
    else:
        filled_values = np.where(np.isnan(all_values), np.inf, all_values)

    T, N = filled_values.shape  # T: total number of rows, N: number of spatial points

    # Use np.argpartition to quickly select indices of the top n values per spatial point
    if(highest):
        top_indices_unsorted = np.argpartition(-filled_values, kth=topN - 1, axis=0)[:topN, :]
    else:
        top_indices_unsorted = np.argpartition(filled_values, kth=topN - 1, axis=0)[:topN, :]
    col_idx = np.arange(N)[None, :]  # shape: (1, N)

    # Gather the top n (unsorted) values.
    top_values_unsorted = filled_values[top_indices_unsorted, col_idx]

    # Sort the top values in descending or ascending order for each spatial point
    if(highest):    # descending
        order = np.argsort(top_values_unsorted, axis=0)[::-1, :]
    else:           # ascending
        order = np.argsort(top_values_unsorted, axis=0)

    top_indices_sorted = np.take_along_axis(top_indices_unsorted, order, axis=0)
    top_values_sorted  = np.take_along_axis(top_values_unsorted, order, axis=0)
    top_times_sorted   = np.take_along_axis(all_times[top_indices_sorted, col_idx], order, axis=0)

    # Convert any -infinity or infinity values back to NaN. Only necessary if the input data contains less than topN timesteps
    if(highest):
        top_values_sorted = np.where(top_values_sorted == -np.inf, np.nan, top_values_sorted)
    else:
        top_values_sorted = np.where(top_values_sorted == np.inf, np.nan, top_values_sorted)

    # Reshape the results to unstack the spatial dimensions
    result_data = xr.DataArray(
        top_values_sorted.reshape(topN, nlat, nlon),
        dims=['rank', 'latitude', 'longitude'],
        coords={'rank': ranks, 'latitude': dataset.latitude, 'longitude': dataset.longitude}
    )
    result_time = xr.DataArray(
        top_times_sorted.reshape(topN, nlat, nlon),
        dims=['rank', 'latitude', 'longitude'],
        coords={'rank': ranks, 'latitude': dataset.latitude, 'longitude': dataset.longitude}
    )

    # Combine the two DataArrays into a single Dataset.
    result = xr.Dataset({
        f"top_{data_var}": result_data,
        f"top_{time_var}": result_time
    })

    return result


def labelSlice(slice_2d, diagonals = True):

    if diagonals:
        event_structure = np.array([[1,1,1],
                                    [1,1,1],
                                    [1,1,1]])
    else:
        event_structure = np.array([[0, 1, 0],
                                    [1, 1, 1],
                                    [0, 1, 0]])

    labeled, _ = label(slice_2d, structure=event_structure)
    return labeled

def getLabeledEvents(mask, latitudeDim = "latitude", longitudeDim = "longitude"):
    labeledEvents = apply_ufunc(
        labelSlice,
        mask,
        input_core_dims=[[latitudeDim,longitudeDim]],
        output_core_dims=[[latitudeDim,longitudeDim]],
        vectorize=True,
        dask="parallelized",
        output_dtypes=[int]
    )
    return labeledEvents

def getConnectedEvents(dataset,variable, threshold, latitudeDim = "latitude", longitudeDim = "longitude", timeDim = "valid_time"):

    allEvents = []

    # Create a mask with the threshold
    mask = dataset[variable] > threshold

    labeledEvents = getLabeledEvents(mask, latitudeDim=latitudeDim, longitudeDim= longitudeDim)

    for idx, t in enumerate(dataset[timeDim]):

        # Get the slice for this timestep
        labeledSlice = labeledEvents.isel({timeDim : idx})

        # Broadcast latitude and longitude to 2D so they match the grid
        lat2d = dataset[latitudeDim].broadcast_like(labeledSlice)
        lon2d = dataset[longitudeDim].broadcast_like(labeledSlice)

        # For each labeled event in this time slice
        for eventLabel in np.unique(labeledSlice.values):

            # Ignore 0, this is background
            if (eventLabel == 0):
                continue

            eventMask = labeledSlice == eventLabel

            # Extract grid cell values that correspond to this event
            eventLatitudes = lat2d.where(eventMask, drop= True)
            eventLongitudes = lon2d.where(eventMask, drop = True)
            eventVariableValues = dataset[variable].isel({timeDim: idx}).where(eventMask, drop = True)

            # Event data to save:
            eventData = {
                'eventTime': str(t.values),
                'eventID': int(eventLabel),
                'minLatitude': float(eventLatitudes.min().item()),
                'maxLatitude': float(eventLatitudes.max().item()),
                'minLongitude': float(eventLongitudes.min().item()),
                'maxLongitude': float(eventLongitudes.max().item()),
                'centroidLatitude': float(eventLatitudes.mean().item()),
                'centroidLongitude': float(eventLongitudes.mean().item()),
                'maxEventValue': float(eventVariableValues.max().item()),
                'meanEventValue': float(eventVariableValues.mean().item()),
                'areaInCells': int(eventMask.sum().item())
            }

            allEvents.append(eventData)

    return allEvents

def getExistingTopTen(resultsFolder, varName):
    try:
        top10Dataset = xr.open_dataset(f"{resultsFolder}/top10{varName}.nc")
        return top10Dataset
    except FileNotFoundError:
        return None
