import unittest
import json
import os
import requests

from unittest.mock import patch
from tasks.json_api_task import FetchDataFromAPI


class TestFetchDataFromAPI(unittest.TestCase):
    def setUp(self):
        self.correct_url_to_json_file = "https://example.com/data.json"
        self.json_local_path = "/tmp/data.json"
        self.data = [{"id": 1, "title": "Post 1"}, {"id": 2, "title": "Post 2"}]
        self.task = FetchDataFromAPI(
            url_to_json_file=self.correct_url_to_json_file,
            json_local_path=self.json_local_path,
        )  # Pass the correct URL to the task

    def test_success_write_file(self):
        self.task.data = self.data
        self.task.write_output()

        # Assert that the file was created
        self.assertTrue(
            os.path.exists(self.json_local_path), "Output file was not created."
        )

        # Read the file contents
        with open(self.json_local_path, "r") as file:
            content = file.read()

        # Assert the file contents

        self.assertEqual(
            content, json.dumps(self.data), "File content does not match expected JSON."
        )

        # Clean up - delete the file
        os.remove(self.json_local_path)

    @patch("requests.get")
    def test_request_file_success(self, mock_requests_get):
        # Mock the response from the API
        mock_response = mock_requests_get.return_value
        mock_response.status_code = 200
        mock_response.json.return_value = self.data

        # Call the method under test
        self.task.request_file()

        # Assert that the API was called with the correct URL
        mock_requests_get.assert_called_once_with(self.correct_url_to_json_file)

        # Assert that the data attribute is set correctly
        self.assertEqual(self.task.data, self.data)

    @patch("requests.get")
    def test_request_file_failure(self, mock_requests_get):
        # Mock a network failure
        mock_requests_get.side_effect = requests.exceptions.RequestException(
            "Network error"
        )

        # Call the method under test and expect an exception to be raised
        with self.assertRaises(Exception) as context:
            self.task.request_file()

        # Assert the error message
        expected_error = f"Failed to fetch data from API on retry: Network error"
        self.assertEqual(str(context.exception), expected_error)


if __name__ == "__main__":
    unittest.main()
