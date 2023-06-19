import luigi
import requests
import json
import logging
import pandas as pd
import numpy as np


from typing import List
from json_api_task import FetchDataFromAPI

logger = logging.getLogger("luigi-interface")


class DataCleaningTask(luigi.Task):
    # output_path = "data.json"  # Output file path
    # json_path = "https://jsonplaceholder.typicode.com/posts"
    json_url = luigi.Parameter()
    json_path = luigi.Parameter()
    stagging_path = luigi.Parameter()

    def requires(self):
        return FetchDataFromAPI(self.json_url, self.json_path)

    def output(self):
        return luigi.LocalTarget(self.stagging_path)

    def run(self):
        self.read_data()

        # Data Validation: Checks for corrupted data that might affect the logic later on
        expected_schema = pd.Series({'userId': np.dtype(int), 'id': np.dtype(
            int), 'title': np.dtype('O'), 'body': np.dtype('O')})
        self.validate_schema(expected_schema)

        str_cols = ["title", "body"]
        self.validate_str_values(str_cols)

        int_cols = ["userId"]
        self.validate_int_values(int_cols)

        # Data completness and consistency

        self.write_output()

    def read_data(self):
        self.df = pd.read_json(self.json_path)

    def validate_schema(self, expected_schema: pd.Series):
        if not self.df.dtypes.equals(expected_schema):
            # Logged as error to indicate its an illegal value that might break production
            logger.error(
                f"Retrieved data has different schema from expected:\nActual schema: {self.df.dtypes}\nExpexted schema: {expected_schema}")
            # Exception raised to indicate that we can not continue operating on this data
            raise Exception("Illegal Argument data schema mismatch")

    def validate_str_values(self, str_cols: List[str]):
        is_valid = True
        for col in str_cols:
            if not self.is_string_series(self.df[col]):
                # Logged as error to indicate its an illegal value that might break production
                logger.error(
                    f"{col} should contain only strings but it doesn't")
                is_valid = False

        return is_valid

    def validate_int_values(self, int_cols: List[str]):
        is_valid = True
        for col in int_cols:
            if not (self.df[col] < 11).all():
                # Logged as error to indicate its an illegal value that might break production
                logger.error(f"{col} contain out of range values")
                is_valid = False

        return is_valid

    def is_string_series(self, s: pd.Series):
        if isinstance(s.dtype, pd.StringDtype):
            # The series was explicitly created as a string series (Pandas>=1.0.0)
            return True
        elif s.dtype == 'object':
            # Object series, check each value
            return all((v is None) or isinstance(v, str) for v in s)
        else:
            return False

    def write_output(self):
        self.df.to_parquet(self.output().path)
