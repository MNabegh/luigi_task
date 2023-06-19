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
        expected_schema = pd.Series({"userId": np.dtype(int), "id": np.dtype(
            int), "title": np.dtype("O"), "body": np.dtype("O")})
        self.validate_schema(expected_schema)

        str_cols = ["title", "body"]
        self.validate_str_values(str_cols)

        int_cols = ["userId"]
        self.validate_int_values(int_cols)

        # Data completness and consistency
        self.check_and_fill_na()

        subset = ["userId", "id"]
        self.check_and_drop_duplicates(subset)

        columns_to_lower = ["title", "body"]
        self.transform_all_strings_to_lower_case(columns_to_lower)

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

    def validate_str_values(self, str_cols: List[str]) -> bool:
        is_valid = True
        for col in str_cols:
            if not self.is_string_series(self.df[col]):
                # Logged as error to indicate its an illegal value that might break production
                logger.error(
                    f"{col} should contain only strings but it doesn't")
                is_valid = False

        return is_valid

    def validate_int_values(self, int_cols: List[str]) -> bool:
        is_valid = True
        for col in int_cols:
            if not (self.df[col] < 11).all():
                # Logged as error to indicate its an illegal value that might break production
                logger.error(f"{col} contain out of range values")
                is_valid = False

        return is_valid

    def is_string_series(self, s: pd.Series) -> bool:
        if isinstance(s.dtype, pd.StringDtype):
            # The series was explicitly created as a string series (Pandas>=1.0.0)
            return True
        elif s.dtype == "object":
            # Object series, check each value
            return all((v is None) or isinstance(v, str) for v in s)
        else:
            return False

    def check_and_fill_na(self) -> pd.DataFrame:
        na_indicies = np.where(self.df.isna().any(axis=1))[0]

        logger.debug(f"{na_indicies} these rows contain NA values")

        # Fill NaN values based on column type
        for col in self.df.columns:
            if self.df[col].dtype == "object":
                self.df.loc[self.df[col].isna(), col] = self.df.loc[self.df[col].isna()
                                                                    ].fillna("Unknown")
            elif self.df[col].dtype == "int64" or self.df[col].dtype == "float64":
                self.df.loc[self.df[col].isna(
                ), col] = self.df.loc[self.df[col].isna()].fillna(0)

    def check_and_drop_duplicates(self, subset: List[str]):
        # Check if the combination of subset columns is unique
        is_unique = ~self.df.duplicated(subset=subset)

        duplicated_indices = np.where(~is_unique)[0]

        logger.debug(f"{duplicated_indices} these rows contained NA values")

        # Drop the duplicated combinations
        self.df = self.df[is_unique]

    def transform_all_strings_to_lower_case(self, columns_to_lower: List[str]):
        self.df[columns_to_lower] = self.df[columns_to_lower].apply(
            lambda x: x.str.lower())

    def write_output(self):
        self.df.to_parquet(self.output().path)
