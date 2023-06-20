import luigi
import requests
import json
import logging

logger = logging.getLogger("luigi-interface")


class FetchDataFromAPI(luigi.Task):
    url_to_json_file = luigi.Parameter()
    json_local_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.json_local_path)

    def run(self):
        logger.info("Starting Luigi task to fetch data from API")
        self.data = None
        self.request_file()

        logger.info("JSON file retrieved, writing to disk")
        if self.data is not None:
            self.write_output()

        logger.info("JSON file has been written to disk")

    def request_file(self):
        try:
            response = requests.get(self.url_to_json_file)
            response.raise_for_status()  # Raise an exception if response status is not 2xx

            self.data = response.json()

        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch data from API on retry: {str(e)}")

        except json.JSONDecodeError as e:
            raise Exception(f"Failed to decode JSON data: {str(e)}")

    def write_output(self):
        # Save the data to a local file
        try:
            with self.output().open("w") as f:
                json.dump(self.data, f)

        except Exception as e:
            logger.error(f"An error occurred while writing to the file: {str(e)}")
