import streamlit as st
import sys
import os
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src', '..')))
import yaml
from src.querying.queryFunctions import getTop10Datasets
from src.config import RESULT_FOLDER, METADATA
from src.utils.unitConversion import getAvailableConversions, getConversionFunction

def getMapFigure(dataset, rank, variable, conversionLambda = None):

    fig = plt.figure(figsize=(14, 6))
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.set_global()

    # Add coastlines
    ax.coastlines()

    dataset = dataset.sel(rank = rank).drop_vars(["rank"])[variable]

    # If conversionLambda is set, apply it for unit conversion
    if conversionLambda:
        dataset = conversionLambda(dataset)

    # Plot the dataset onto a map
    cbar = dataset.plot.pcolormesh(ax=ax, transform=ccrs.PlateCarree(), x="longitude", y="latitude", add_colorbar=True)

    # Remove colorbar label
    cbar.colorbar.set_label("")
    return fig

@st.cache_data
def loadMetadata():
    with open(METADATA, "r") as file:
        metadata = yaml.safe_load(file)
    return metadata


def app():

    st.title("Visualization of top 10 ranks")

    metadata = loadMetadata()
    displayNames, datasets, options = {}, {}, []

    # Extract name, dataset and display name
    for top10 in getTop10Datasets():
        # Get internal name and dataset
        name, dataset = top10

        # Save display name and internal name in a dict for later reversal
        displayNames[metadata[name]["displayName"]] = name

        # Save dataset in a dict under internal name
        datasets[name] = dataset

        # Add display name to options that are going to be displayed
        options.append(metadata[name]["displayName"])

    col1, col2 = st.columns(2)
    with col1:
        topTenSelection = st.selectbox("Select a top ten to view", options)

    # Get internal name for selection
    variable = displayNames[topTenSelection]

    # Get the unit of the currently selected event
    unit = metadata[variable]['unit']

    # Get conversion options for this unit
    unitOptions = getAvailableConversions(unit)

    # Add the original unit, so switching back is possible
    unitOptions.append(unit)

    with col2:
        unitSelect = st.selectbox("Unit: ", unitOptions, index=len(unitOptions) - 1)

    # If selected unit is the unit the dataset is in, we dont need to convert anything
    if unitSelect == unit:
        conversionLambda = None
    # Otherwise get the conversion function
    else:
        conversionLambda = getConversionFunction(unit, unitSelect)


    rank = st.slider("Select the rank to display",1,10,1)


    fig = getMapFigure(datasets.get(variable), rank, f'top_{metadata[variable]["datasetValueName"]}', conversionLambda =conversionLambda )
    st.pyplot(fig = fig)
    st.subheader(f"Data description - {topTenSelection}")
    st.write(metadata[variable]["description"])

if __name__ == "__main__":
    app()
