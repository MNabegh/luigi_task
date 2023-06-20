import luigi
import logging
import pandas as pd
import numpy as np


from typing import List
from data_cleaning_task import DataCleaningTask

logger = logging.getLogger("luigi-interface")


class DataTransformationTask(luigi.Task):
    url_to_json_file = luigi.Parameter()
    json_path = luigi.Parameter()
    staging_path = luigi.Parameter()
    transformation_path = luigi.Parameter()

    def requires(self):
        return DataCleaningTask(self.url_to_json_file, self.json_path, self.staging_path)

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

        self.df = pd.read_parquet(self.staging_path)

    def transform_data(self, old_col, new_cols):
        """
        This function assumes that the text in the column {old_col} has specific number of lines that we want to split
        into multiple columns represented by {new_cols}.
        """
        splitted_df = self.df[old_col].str.split(
            "\n", expand=True)

        logger.debug(
            f"Longest posts have {splitted_df.shape[1]} lines while expected is {len(new_cols)}")

        # If the input has more lines than the number of expected columns in {new_cols}
        # We take the first n lines corresponding to the expected number
        self.df[new_cols] = splitted_df.iloc[:, :len(new_cols)]

    def write_output(self):
        self.df.to_parquet(self.output().path)
