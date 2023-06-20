import luigi
import logging
import pandas as pd
import numpy as np


from json_api_task import FetchDataFromAPI

logger = logging.getLogger("luigi-interface")


class DataCleaningTask(luigi.Task):
    url_to_json_file = luigi.Parameter()
    json_path = luigi.Parameter()
    staging_path = luigi.Parameter()

    def requires(self):
        return FetchDataFromAPI(self.url_to_json_file, self.json_path)

    def output(self):
        return luigi.LocalTarget(self.staging_path)

    def run(self):
        logger.info("Starting Luigi task to Validate and clean raw data")
        self.read_data()

        logger.info(
            "Validating data format is as expected and will not break production before loading it for business"
        )

        expected_schema = pd.Series({
            "userId": np.dtype(int),
            "id": np.dtype(int),
            "title": np.dtype("O"),
            "body": np.dtype("O")
        })
        self.validate_schema(expected_schema)

        str_cols = ["title", "body"]
        self.validate_str_values(str_cols)

        int_cols = ["userId"]
        self.validate_int_values(int_cols)

        logger.info("Check and ensure data completness and consistency")
        self.check_and_fill_na()

        unique_columns_combination = ["userId", "id"]
        self.check_and_drop_duplicates(unique_columns_combination)

        lower_case_columns = ["title", "body"]
        self.transform_all_strings_to_lower_case(lower_case_columns)

        logger.info("Data cleaning is complete, writing to stagging layer")
        self.write_output()
        logger.info("Cleaned data has been written to the stagging layer")

    def read_data(self):
        self.df = pd.read_json(self.json_path)

    def validate_schema(self, expected_schema: pd.Series):
        if not self.df.dtypes.equals(expected_schema):
            logger.error(
                f"Retrieved data has different schema from expected:\nActual schema: {self.df.dtypes}\nExpexted schema: {expected_schema}"
            )
            raise Exception("Illegal Argument data schema mismatch")

    def validate_str_values(self, str_cols: list[str]):

        for col in str_cols:
            if not self.is_string_series(self.df[col]):
                # Logged as error to indicate its an illegal value that might break production
                logger.error(
                    f"{col} should contain only strings but it doesn't")
                raise Exception(
                    "Illegal Argument non string data when it is expected to be string")

    def validate_int_values(self, int_cols: list[str]):
        for col in int_cols:
            if not (self.df[col] < 11).all():
                # Logged as error to indicate its an illegal value that might break production
                logger.error(f"{col} contain out of range values")
                raise Exception(
                    "Illegal Argument integer values are out of range")

    def is_string_series(self, series: pd.Series) -> bool:
        if isinstance(series.dtype, pd.StringDtype):
            return True
        elif series.dtype == "object":
            return all((value is None) or isinstance(value, str) for value in series)
        else:
            return False

    def check_and_fill_na(self):
        na_indicies = np.where(self.df.isna().any(axis=1))[0]

        logger.debug(f"{na_indicies} these rows contain NA values")

        # Fill NaN values based on column type
        for col in self.df.columns:
            if self.is_object_column(col):
                self.replace_na_with_unknown_in_column(col)
            elif self.is_numeric_column(col):
                self.replace_na_with_zero_in_column(col)

    def is_object_column(self, col: pd.Series) -> bool:
        return self.df[col].dtype == "object"

    def is_numeric_column(self, col: pd.Series) -> bool:
        return self.df[col].dtype == "int64" or self.df[col].dtype == "float64"

    def replace_na_with_unknown_in_column(self, col: pd.Series):
        self.df.loc[self.df[col].isna(), col] = \
            self.df.loc[self.df[col].isna()].fillna("Unknown")

    def replace_na_with_zero_in_column(self, col: pd.Series):
        self.df.loc[self.df[col].isna(), col] = \
            self.df.loc[self.df[col].isna()].fillna(0)

    def check_and_drop_duplicates(self, unique_columns_combination: list[str]):
        # Check if the combination of subset columns is unique
        is_row_signature_unique = ~self.df.duplicated(
            subset=unique_columns_combination)

        rows_with_duplicated_signature_indices = np.where(
            ~is_row_signature_unique)[0]

        logger.debug(
            f"{rows_with_duplicated_signature_indices} these rows have duplicated signature")

        # Drop the duplicated combinations
        self.df = self.df[is_row_signature_unique]

    def transform_all_strings_to_lower_case(self, columns_to_lower: list[str]):
        self.df[columns_to_lower] = self.df[columns_to_lower].apply(
            lambda x: x.str.lower())

    def write_output(self):
        self.df.to_parquet(self.output().path)
