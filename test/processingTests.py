import unittest
import numpy as np
import xarray as xr
from src.processing.processing_functions import update_top_n, labelSlice, getLabeledEvents, getConnectedEvents
import warnings

class TestProcessingFunctions(unittest.TestCase):

    def setUp(self):
        self.latitudes = np.linspace(-90, 90, 5)
        self.longitudes = np.linspace(-180, 180, 5)
        self.times = np.array(['2023-01-01', '2023-01-02', '2023-01-03'], dtype='datetime64')

        data = np.random.rand(len(self.times), len(self.latitudes), len(self.longitudes))

        # suppress nano-second precision conversion user warning
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.dataset = xr.Dataset(
                {
                    "test_var": (['valid_time', 'latitude', 'longitude'], data)
                },
                coords={
                    "valid_time": self.times,
                    "latitude": self.latitudes,
                    "longitude": self.longitudes
                }
        )

    def test_update_top_n(self):
        result = update_top_n(self.dataset, "test_var")
        self.assertIn("top_test_var", result, "Output dataset should contain top values")
        self.assertIn("top_valid_time", result, "Output dataset should contain corresponding times")
        self.assertEqual(result[f"top_test_var"].shape[0], 10, "Top values dataset should have rank 10")

    def test_labelSlice(self):
        testArray = np.array([[1, 1, 0], [0, 1, 1], [1, 0, 0]])
        labeled = labelSlice(testArray, diagonals=True)
        labeledCross = labelSlice(testArray, diagonals=False)
        self.assertTrue(labeled.max() == 1, "Labeling with diagonals should detect 1 connected components")
        self.assertTrue(labeledCross.max() == 2, "Labeling with cross should detect 2 connected components")

    def test_getLabeledEvents(self):
        # suppress nano-second precision conversion user warning
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            mask = xr.DataArray(
                np.random.randint(0, 2, (len(self.times), len(self.latitudes), len(self.longitudes))),
                dims=["valid_time", "latitude", "longitude"],
                coords={"valid_time": self.times, "latitude": self.latitudes, "longitude": self.longitudes}
            )
        labeledEvents = getLabeledEvents(mask)
        self.assertEqual(labeledEvents.shape, mask.shape,
                         "Labeled events should have the same shape as the input mask")

    def test_getConnectedEvents(self):
        threshold = 0.5
        events = getConnectedEvents(self.dataset, "test_var", threshold)
        self.assertIsInstance(events, list, "Output should be a list of events")

    def test_getConnectedEvents_empty(self):

        empty_dataset = xr.Dataset({"test_var": (['valid_time', 'latitude', 'longitude'], np.zeros(
            (len(self.times), len(self.latitudes), len(self.longitudes))))})
        events = getConnectedEvents(empty_dataset, "test_var", 1.0)
        self.assertEqual(len(events), 0, "Should return an empty list when no events exceed the threshold")


if __name__ == '__main__':
    unittest.main()
