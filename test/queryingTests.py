import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from xarray import Dataset
from src.querying.queryFunctions import (
    get_lat_lon, getCityCoords, getTop10City, getTop10Datasets,
    getAllTopTensForCity, getTopTenForCityForEventType,
    getAllRecordsForCity, getAllRecordsForCityAndEventType,
    groupEventsByTime, geoRound
)


class TestQueryFunctions(unittest.TestCase):

    @patch("src.querying.queryFunctions.Nominatim.geocode")
    def test_get_lat_lon_valid_city(self, mock_geocode):
        mock_geocode.return_value = MagicMock(latitude=40.7128, longitude=-74.0060)
        lat, lon = get_lat_lon("New York")
        self.assertEqual(lat, 40.7128)
        self.assertEqual(lon, -74.0060)

    @patch("src.querying.queryFunctions.Nominatim.geocode")
    def test_get_lat_lon_invalid_city(self, mock_geocode):
        mock_geocode.return_value = None
        lat, lon = get_lat_lon("FakeCity")
        self.assertIsNone(lat)
        self.assertIsNone(lon)

    def test_geoRound(self):
        self.assertEqual(geoRound(40.9), 40.75)
        self.assertEqual(geoRound(-74.1), -74.25)

    @patch("src.querying.queryFunctions.get_lat_lon", return_value=(40.7128, -74.0000))
    def test_getCityCoords(self, mock_get_lat_lon):
        lat, lon = getCityCoords("New York")
        self.assertEqual(lat, 40.5)
        self.assertEqual(lon, 286.0)  # Converted from -74.0 to 286.0

    @patch("src.querying.queryFunctions.getCityCoords")
    def test_getTop10City(self, mock_getCityCoords):
        # Mock the city coordinates function to return a fixed lat/lon
        mock_getCityCoords.return_value = (40.75, -73.99)

        # Create a mock dataset
        dataset_mock = MagicMock(spec=Dataset)

        # Mock the sel method to return another mock object
        mock_dataframe = pd.DataFrame({
            "latitude": [40.75] * 3,
            "longitude": [-73.99] * 3,
            "value": [1, 2, 3]
        })

        dataset_mock.sel.return_value.to_dataframe.return_value = mock_dataframe

        # Call the function under test
        result = getTop10City(dataset_mock, "New York")

        # Assertions
        self.assertEqual(len(result), 3)
        self.assertIn("value", result.columns)
        self.assertNotIn("latitude", result.columns)
        self.assertNotIn("longitude", result.columns)

    @patch("src.querying.queryFunctions.glob.glob")
    @patch("src.querying.queryFunctions.open_dataset")
    def test_getTop10Datasets(self, mock_open_dataset, mock_glob):
        mock_glob.return_value = ["/path/to/top10event.nc"]
        mock_open_dataset.return_value = Dataset()
        datasets = getTop10Datasets()
        self.assertEqual(len(datasets), 1)

    @patch("src.querying.queryFunctions.getTop10Datasets")
    @patch("src.querying.queryFunctions.getTop10City")
    def test_getAllTopTensForCity(self, mock_getTop10City, mock_getTop10Datasets):
        mock_getTop10Datasets.return_value = [["event", Dataset()]]
        mock_getTop10City.return_value = pd.DataFrame({"value": [1, 2, 3]})
        result = getAllTopTensForCity("New York")
        self.assertEqual(len(result), 1)

    @patch("src.querying.queryFunctions.open_dataset")
    def test_getTopTenForCityForEventType_file_not_found(self, mock_open_dataset):
        mock_open_dataset.side_effect = FileNotFoundError
        with self.assertRaises(FileNotFoundError):
            getTopTenForCityForEventType("New York", "event")

    @patch("src.querying.queryFunctions.sqlite3.connect")
    def test_getAllRecordsForCity(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_records = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.return_value = mock_records
        mock_records.fetchall.return_value = [(1, "wind", "2024-01-01", 40, -40, -73, -73, 40, -73, 10, 10, 1)]

        df = getAllRecordsForCity("New York")
        self.assertFalse(df.empty)
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_groupEventsByTime(self):
        df = pd.DataFrame({
            "eventType": ["rain", "rain", "rain", "heatwave"],
            "eventTime": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-04", "2024-02-01"])
        })
        result = groupEventsByTime(df)
        self.assertEqual(len(result), 3)


if __name__ == "__main__":
    unittest.main()
