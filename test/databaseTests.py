import os
import sqlite3
import unittest

from src.processing.databaseFunctions import splitFilename, createProcessingDatabase, updateProcessingStatus, \
    createResultDatabase, insertEventsIntoDatabase, resultDatabaseRecordsToDataframe, updateProcessingDatabase


class TestProcessingDatabase(unittest.TestCase):

    def setUp(self) -> None:
        """
        Setup test environment
        :return:
        """
        self.testProcessingDatabase = "testProcessing.db"
        self.testResultDatabase = "testResult.db"
        self.testDirectory = "testFiles/"

        os.makedirs(self.testDirectory, exist_ok=True)

        with open(os.path.join(self.testDirectory,"var_2023.nc"), "w") as f:
            f.write("test file")

    def tearDown(self) -> None:
        """
        Clean up after testing.
        :return:
        """

        if os.path.exists(self.testProcessingDatabase):
            os.remove(self.testProcessingDatabase)
        if os.path.exists(self.testResultDatabase):
            os.remove(self.testResultDatabase)
        for file in os.listdir(self.testDirectory):
            os.remove(os.path.join(self.testDirectory, file))
        os.rmdir(self.testDirectory)

    def testFilenameSplitting(self):

        self.assertEqual(splitFilename("var_2023.nc"), ("var","2023"))
        self.assertEqual(splitFilename("thisIsGonnaBeAnError.nc"), (None, None))
        self.assertEqual(splitFilename("var_202323.nc"), (None, None))

    def testCreateProcessingDatabase(self):

        createProcessingDatabase(self.testDirectory, self.testProcessingDatabase)

        connection = sqlite3.connect(self.testProcessingDatabase)
        cursor = connection.cursor()

        records = cursor.execute("SELECT * FROM processing").fetchall()

        cursor.close()
        connection.close()

        self.assertEqual(len(records),1)
        self.assertEqual(records[0][1:], ("var", 2023, "unprocessed"))


    def testUpdateProcessingDatabase(self):

        createProcessingDatabase("", self.testProcessingDatabase)

        connection = sqlite3.connect(self.testProcessingDatabase)
        cursor = connection.cursor()

        records = cursor.execute("SELECT * FROM processing").fetchall()

        self.assertEqual(len(records),0)

        updateProcessingDatabase(self.testDirectory, self.testProcessingDatabase)

        records = cursor.execute("SELECT * FROM processing").fetchall()
        self.assertEqual(len(records),1)

        cursor.close()
        connection.close()

    def testUpdateProcessingStatus(self):
        createProcessingDatabase(self.testDirectory, self.testProcessingDatabase)
        updateProcessingStatus(self.testProcessingDatabase, 2023, "var", "processed")

        connection = sqlite3.connect(self.testProcessingDatabase)
        cursor = connection.cursor()

        status = cursor.execute("SELECT status FROM processing WHERE year = 2023 and variable = 'var'").fetchone()[0]

        cursor.close()
        connection.close()

        self.assertEqual(status, "processed")

    def testCreateResultDatabase(self):

        createResultDatabase(self.testResultDatabase)

        connection = sqlite3.connect(self.testResultDatabase)
        cursor = connection.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='thresholdResults'")
        table_exists = cursor.fetchone()

        cursor.close()
        connection.close()

        self.assertIsNotNone(table_exists)

    def test_insertEventsIntoDatabase(self):
        createResultDatabase(self.testResultDatabase)
        events = [{
            "eventTime": "2024-01-01",
            "minLatitude": 10.0, "maxLatitude": 20.0,
            "minLongitude": 30.0, "maxLongitude": 40.0,
            "centroidLatitude": 15.0, "centroidLongitude": 35.0,
            "maxEventValue": 50.0, "meanEventValue": 25.0,
            "areaInCells": 100
        }]
        insertEventsIntoDatabase(self.testResultDatabase, "wind", events)

        connection = sqlite3.connect(self.testResultDatabase)
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM thresholdResults")

        records = cursor.fetchall()

        cursor.close()
        connection.close()

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0][1:],
                         ("wind", "2024-01-01", 10.0, 20.0, 30.0, 40.0, 15.0, 35.0, 50.0, 25.0, 100))

    def test_resultDatabaseRecordsToDataframe(self):
        records = [(1, "wind", "2024-01-01", 10.0, 20.0, 30.0, 40.0, 15.0, 35.0, 50.0, 25.0, 100)]
        df = resultDatabaseRecordsToDataframe(records)

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["eventType"], "wind")
        self.assertEqual(str(df.iloc[0]["eventTime"]), "2024-01-01 00:00:00")
