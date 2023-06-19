import unittest
import pandas as pd
import numpy as np
from tasks.data_cleaning_task import DataCleaningTask


class TestYourClass(unittest.TestCase):
    def setUp(self):
        # Create a sample DataFrame for testing
        data = {
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c'],
            'col3': [10, 20, 30]
        }
        self.df = pd.DataFrame(data)
        # Initialize YourClass with the DataFrame
        self.obj = DataCleaningTask('url', 'path1', 'path2')
        self.obj.df = self.df

    def test_validate_schema(self):
        # Define the expected schema
        expected_schema = pd.Series(
            {'col1': np.dtype(int), 'col2': np.dtype('O'), 'col3': np.dtype(int)})

        # Test when the schema matches the expected schema
        self.assertIsNone(self.obj.validate_schema(expected_schema))

        # Test when the schema does not match the expected schema
        wrong_schema = pd.Series(
            {'col1': np.dtype(int), 'col2': np.dtype(int), 'col3': np.dtype(int)})

        with self.assertRaises(Exception) as context:
            self.obj.validate_schema(wrong_schema)

    def test_validate_str_values(self):
        # Test when all string columns contain only strings
        str_cols = ['col2']
        self.assertTrue(self.obj.validate_str_values(str_cols))

        # Test when a string column contains non-string values
        invalid_cols = ['col1']
        self.assertFalse(self.obj.validate_str_values(invalid_cols))

    def test_validate_int_values(self):
        # Test when all integer columns contain values within the range
        int_cols = ['col1']
        self.assertTrue(self.obj.validate_int_values(int_cols))

        # Test when an integer column contains out-of-range values
        invalid_cols = ['col3']
        self.assertFalse(self.obj.validate_int_values(invalid_cols))

    def test_is_string_series(self):
        # Test when the series is a string series
        string_series = pd.Series(['a', 'b', 'c'], dtype="string")
        self.assertTrue(self.obj.is_string_series(string_series))

        # Test when the series is an object series with string values
        object_series_valid = pd.Series(['a', 'b', 'c'])
        self.assertTrue(self.obj.is_string_series(object_series_valid))

        # Test when the series is an object series with non string values
        object_series_invalid = pd.Series(['a', 'b', 'c', 1, 2, 3])
        self.assertFalse(self.obj.is_string_series(object_series_invalid))

        # Test when the series is an object series with non-string values
        non_string_series = pd.Series([1, 2, 3])
        self.assertFalse(self.obj.is_string_series(non_string_series))


if __name__ == '__main__':
    unittest.main()
