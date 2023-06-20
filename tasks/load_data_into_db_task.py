import luigi
import sqlite3
import pandas as pd

from data_transformation_task import DataTransformationTask
from create_table_task import CreateTableTask


class LoadDataToDatabaseTask(luigi.Task):
    url_to_json_file = luigi.Parameter()
    json_path = luigi.Parameter()
    staging_path = luigi.Parameter()
    transformation_path = luigi.Parameter()
    db_connection_string = luigi.Parameter(default="db/example.db")
    table_name = luigi.Parameter(default="transformed_data_table")

    def requires(self):
        return DataTransformationTask(
            self.url_to_json_file,
            self.json_path,
            self.staging_path,
            self.transformation_path
        )

    def output(self):
        # Output target to mark the task as complete
        return DatabaseLoadedTarget(self.db_connection_string, self.table_name)

    def run(self):
        # Read the transformed data from the Parquet file
        transformed_data = pd.read_parquet(self.transformation_path)

        # Connect to the database
        conn = sqlite3.connect(self.db_connection_string)
        cursor = conn.cursor()

        # Insert the cleaned data into the database table
        transformed_data.to_sql(self.table_name, conn,
                                if_exists='replace', index=False)

        # Commit the changes and close the connection
        conn.commit()
        conn.close()


class DatabaseLoadedTarget(luigi.Target):
    def __init__(self, db_connection_string, table_name):
        self.db_connection_string = db_connection_string
        self.table_name = table_name

    def exists(self):
        conn = sqlite3.connect(self.db_connection_string)
        cursor = conn.cursor()

        # Check if the table contains any data
        cursor.execute(f"SELECT COUNT(*) FROM {self.table_name};")
        result = cursor.fetchone()[0]

        conn.close()

        return result > 0
