import streamlit as st
import sys
import os
import yaml
import folium
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src', '..')))
from src.config import METADATA
from src.querying.queryFunctions import getAllRecordsForCity, getAllTopTensForCity, groupEventsByTime, get_lat_lon
from src.utils.unitConversion import getAvailableConversions,getConversionFunction

@st.cache_data
def getCityLatLon(city):
    return get_lat_lon(city)

@st.cache_data
def loadMetadata():
    with open(METADATA, "r") as file:
        metadata = yaml.safe_load(file)
    return metadata

@st.cache_data
def getCityRecords(city):
    return getAllRecordsForCity(city)

@st.cache_data
def getMatchingRecords(event, records):
    return     records[(records["eventType"] == event["eventType"]) & (
                (records["eventTime"] >= event["startTime"]) & (records["eventTime"] <= event["endTime"]))]

@st.cache_data
def createMap(city, df, selectedIndex):
    # Start with a map centered around the dataset's lat/lon
    mapCenter = getCityLatLon(city)
    m = folium.Map(location=mapCenter, zoom_start=5)

    # Add a bounding box for the event
    row = df.iloc[selectedIndex]

    # Define the 4 corners of the box (based on the latitude/longitude grid)
    # The box will be a 0.25 x 0.25 degree grid. Adding 0.25 to maximum latitude and longitude, because otherwise it would only be the bottom-left point of the grid box.
    lat_min = row["minLatitude"]
    lat_max = row["maxLatitude"] + 0.25
    lon_min = row["minLongitude"]
    lon_max = row["maxLongitude"] + 0.25

    # Create a Rectangle to represent the grid box and add it to the map
    folium.Rectangle(
        bounds=[[lat_min, lon_min], [lat_max, lon_max]],
        color="red",
        fill=True,
        fill_opacity=0.5,
        weight=0
    ).add_to(m)

    return m

def displayEventData(city):

    st.header("Thresholds")

    # Get all records where the city lies in the correct grid cell
    records = getCityRecords(city)

    # Group events that (time-wise) associated events together
    grouped = groupEventsByTime(records)

    select = st.dataframe(grouped, selection_mode="single-row", on_select="rerun")

    # If an event has been selected, show further data, otherwise prompt the user to select an event
    try:
        # Get all events that belong to the selected records and display them
        event = grouped.iloc[select.selection.rows[0]]
        eventRecords = getMatchingRecords(event, records)
        st.dataframe(eventRecords)

        # If the event only has one time-step, we don't need a slider to switch through them
        if(len(eventRecords)>1):
            selected_index = st.slider("Select Layer", 0, len(eventRecords) - 1, 0)
        else:
            selected_index = 0

        # Create an interactive map with grid boxes
        m = createMap(city, eventRecords.reindex(), selected_index)

        # Save the folium map as an HTML file
        map_html = 'map.html'
        m.save(map_html)

        # Display the map in Streamlit using st.components.v1.html()
        with open(map_html, 'r') as f:
            map_html_content = f.read()
        st.components.v1.html(map_html_content, height=500)

        # Optionally, remove the saved map file after displaying
        os.remove(map_html)

    except IndexError:
        st.write("Select a record to view available data")

@st.cache_data
def getTopTenData(city):
    return getAllTopTensForCity(city)


def displayTopTenData(city, metadata):

    st.header("Top Ten")

    displayNames, datasets, options = {}, {}, []

    # Extract name, dataset and display name
    for top10 in getTopTenData(city):
        # Get internal name and dataset
        name,dataset = top10

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

    # Get appropriate dataset
    df = datasets[displayNames[topTenSelection]]

    # Get name of the value inside the dataset. (always top_something)
    valuename = f"top_{metadata[variable]['datasetValueName']}"

    # If conversionLambda is set, apply it
    if conversionLambda:
        df[valuename] = conversionLambda(df[valuename])
    # Display table
    st.table(df)


def app():
    st.title("Extreme weather database")
    metadata = loadMetadata()
    city = st.text_input("Enter a city", "Köln")
    displayEventData(city)
    displayTopTenData(city, metadata)

if __name__ == "__main__":
    app()


