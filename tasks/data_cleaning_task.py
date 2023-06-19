import luigi
import requests
import json
import logging

from json_api_task import FetchDataFromAPI

logger = logging.getLogger("luigi-interface")


class DataCleaningTask(luigi.Task):
    # output_path = "data.json"  # Output file path
    # json_path = "https://jsonplaceholder.typicode.com/posts"
    json_url = luigi.Parameter()
    json_path = luigi.Parameter()
    stagging_path = luigi.Parameter()

    def requires(self):
        return FetchDataFromAPI(json_url, json_path)

    def output(self):
        return luigi.LocalTarget(self.stagging_path)

    def run(self):
        pass
