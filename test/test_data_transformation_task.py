import unittest
import pandas as pd
from tasks.data_transformation_task import DataTransformationTask


class DataTransformationTaskTest(unittest.TestCase):

    def setUp(self):
        # Create a sample DataFrame for testing
        data = {"Text": ["Line 1\nLine 2", "Line 3\nLine 4\nLine 5\nLine6"]}
        self.df = pd.DataFrame(data)
        self.transformer = DataTransformationTask(
            "dfsa", "rfar", "jre", "rejag")

        self.transformer.df = self.df

    def test_transform_data(self):
        # Define the expected results
        expected_df = pd.DataFrame({"Text": ["Line 1\nLine 2", "Line 3\nLine 4\nLine 5\nLine6"],
                                   "Line 1": ["Line 1", "Line 3"],
                                    "Line 2": ["Line 2", "Line 4"],
                                    "Line 3": [None, "Line 5"]})

        # Call the transform_data method
        self.transformer.transform_data("Text", ["Line 1", "Line 2", "Line 3"])

        # Check if the transformed DataFrame is as expected
        self.assertTrue(self.df.equals(expected_df))


if __name__ == "__main__":
    unittest.main()
