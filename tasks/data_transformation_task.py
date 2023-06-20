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
        logger.info("Starting Luigi task to transform data into business logic")
        self.read_data()

        logger.info("Perform transformation step")
        old_col = "body"
        new_cols = ["body_1", "body_2", "body_3", "body_4"]

        self.transform_data(old_col, new_cols)

        logger.info(
            "Data transformation is complete, writing to business layer"
        )
        self.write_output()
        logger.info("Transformed data has been written to the business layer")

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
