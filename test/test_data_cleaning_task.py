import unittest
import pandas as pd
import numpy as np
from tasks.data_cleaning_task import DataCleaningTask


class TestYourClass(unittest.TestCase):
    def setUp(self):
        # Create a sample DataFrame for testing
        data = {
            "col1": [1, 2, 3, 1],
            "col2": ["A", "B", "c", "D"],
            "col3": [10, 20, np.nan, 10],
            "col4": ["apple", "Orange", None, "Pear"]
        }
        self.df = pd.DataFrame(data)
        # Initialize YourClass with the DataFrame
        self.obj = DataCleaningTask("url", "path1", "path2")
        self.obj.df = self.df

    def test_validate_schema(self):
        # Define the expected schema
        expected_schema = pd.Series(
            {"col1": np.dtype(int), "col2": np.dtype("O"), "col3": np.dtype(float), "col4": np.dtype("O")})

        # Test when the schema matches the expected schema
        self.assertIsNone(self.obj.validate_schema(expected_schema))

        # Test when the schema does not match the expected schema
        wrong_schema = pd.Series(
            {"col1": np.dtype(int), "col2": np.dtype(int), "col3": np.dtype(int), "col4": np.dtype("O")})

        with self.assertRaises(Exception) as context:
            self.obj.validate_schema(wrong_schema)

    def test_validate_str_values(self):
        # Test when all string columns contain only strings
        str_cols = ["col2"]
        self.assertTrue(self.obj.validate_str_values(str_cols))

        # Test when a string column contains non-string values
        invalid_cols = ["col1"]
        self.assertFalse(self.obj.validate_str_values(invalid_cols))

    def test_validate_int_values(self):
        # Test when all integer columns contain values within the range
        int_cols = ["col1"]
        self.assertTrue(self.obj.validate_int_values(int_cols))

        # Test when an integer column contains out-of-range values
        invalid_cols = ["col3"]
        self.assertFalse(self.obj.validate_int_values(invalid_cols))

    def test_is_string_series(self):
        # Test when the series is a string series
        string_series = pd.Series(["a", "b", "c"], dtype="string")
        self.assertTrue(self.obj.is_string_series(string_series))

        # Test when the series is an object series with string values
        object_series_valid = pd.Series(["a", "b", "c"])
        self.assertTrue(self.obj.is_string_series(object_series_valid))

        # Test when the series is an object series with non string values
        object_series_invalid = pd.Series(["a", "b", "c", 1, 2, 3])
        self.assertFalse(self.obj.is_string_series(object_series_invalid))

        # Test when the series is an object series with non-string values
        non_string_series = pd.Series([1, 2, 3])
        self.assertFalse(self.obj.is_string_series(non_string_series))

    def test_check_and_fill_na(self):
        # Perform check and fill NA values
        self.obj.check_and_fill_na()

        # Assert that NA values have been filled
        self.assertFalse(self.obj.df.isna().any().any(), self.obj.df)

        # Assert specific values have been filled
        self.assertEqual(self.obj.df["col4"][2], "Unknown")
        self.assertEqual(self.obj.df["col3"][2], 0)

    def test_check_and_drop_duplicates(self):
        # Assert that the dataframe has 4 rows
        self.assertEqual(self.obj.df.shape[0], 4)

        # Perform check and drop duplicates
        self.obj.check_and_drop_duplicates(subset=["col1", "col3"])

        # Assert that the dataframe has 3 rows
        self.assertEqual(self.obj.df.shape[0], 3)

        # Assert that the dataframe has 3 rows
        self.assertEquals(self.obj.df["col1"].to_list(), [1, 2, 3])

    def test_transform_all_strings_to_lower_case(self):
        self.assertEqual(self.obj.df["col2"][0], "A")
        self.assertEqual(self.obj.df["col2"][1], "B")
        self.assertEqual(self.obj.df["col2"][3], "D")
        self.assertEqual(self.obj.df["col4"][1], "Orange")
        self.assertEqual(self.obj.df["col4"][3], "Pear")

        # Perform transformation of selected columns to lowercase
        self.obj.transform_all_strings_to_lower_case(
            columns_to_lower=["col2", "col4"])

        # Assert that selected columns have been transformed to lowercase
        self.assertEqual(self.obj.df["col2"][0], "a")
        self.assertEqual(self.obj.df["col2"][1], "b")
        self.assertEqual(self.obj.df["col2"][2], "c")
        self.assertEqual(self.obj.df["col2"][3], "d")
        self.assertEqual(self.obj.df["col4"][0], "apple")
        self.assertEqual(self.obj.df["col4"][1], "orange")
        self.assertEqual(self.obj.df["col4"][3], "pear")


if __name__ == "__main__":
    unittest.main()
