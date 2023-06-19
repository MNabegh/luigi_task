import luigi
import requests
import json
import logging

logger = logging.getLogger("luigi-interface")


class FetchDataFromAPI(luigi.Task):
    # output_path = "data.json"  # Output file path
    # json_path = "https://jsonplaceholder.typicode.com/posts"
    json_url = luigi.Parameter()
    output_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.output_path)

    def run(self):
        logger.info("Starting Luigi task to fetch data from API")
        self.request_file()
        self.write_output()

    def request_file(self):
        try:
            response = requests.get(self.json_url)
            response.raise_for_status()  # Raise an exception if response status is not 2xx

            self.data = response.json()

        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            # Handle network failures or JSON decoding errors
            logger.warning("Network Failure")
            raise Exception(f"Failed to fetch data from API on retry: {str(e)}")

    def write_output(self):
        # Save the data to a local file
        with self.output().open("w") as f:
            json.dump(self.data, f)
