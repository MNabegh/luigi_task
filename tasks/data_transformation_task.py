import luigi
import logging
import pandas as pd
import numpy as np


from typing import List
from data_cleaning_task import DataCleaningTask

logger = logging.getLogger("luigi-interface")


class DataTransformationTask(luigi.Task):
    # output_path = "data.json"  # Output file path
    # json_path = "https://jsonplaceholder.typicode.com/posts"
    json_url = luigi.Parameter()
    json_path = luigi.Parameter()
    stagging_path = luigi.Parameter()
    transformation_path = luigi.Parameter()

    def requires(self):
        return DataCleaningTask(self.json_url, self.json_path, self.stagging_path)

    def output(self):
        return luigi.LocalTarget(self.transformation_path)

    def run(self):
        self.read_data()

        # After checking every post has 4 lines, altough this is not dynamic.
        # It is just to show some useful transformation that constructs denormalised data
        old_col = "body"
        new_cols = ["body_1", "body_2", "body_3", "body_4"]

        self.transform_data(old_col, new_cols)

        self.write_output()

    def read_data(self):
        self.df = pd.read_parquet(self.stagging_path)

    def transform_data(self, old_col, new_cols):
        splitted_df = self.df[old_col].str.split(
            "\n", expand=True)

        logger.debug(
            f"Longest posts have {splitted_df.shape[1]} lines while expected is {len(new_cols)}")

        # The subscript at the end to make it more robust in case we have more than 4 lines
        self.df[new_cols] = splitted_df.iloc[:, :len(new_cols)]

    def write_output(self):
        self.df.to_parquet(self.output().path)
