import luigi
import sqlite3

from create_db_task import CreateDatabaseTask


class CreateTableTask(luigi.Task):
    db_connection_string = luigi.Parameter(default="db/example.db")
    table_name = "transformed_data_table"

    def output(self):
        # Output target to mark the task as complete
        return TableExistsTarget(self.db_connection_string, self.table_name)

    def run(self):
        # Connect to the database
        conn = sqlite3.connect(self.db_connection_string)
        cursor = conn.cursor()

        # Create the table
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            userId INTEGER,
            Id INTEGER,
            title VARCHAR,
            body VARCHAR
        )
        """
        try:
            cursor.execute(create_table_query)
            conn.commit()
        except Exception as e:
            # Handle any exceptions that may occur during table creation
            raise Exception(f"Error creating table: {str(e)}")
        finally:
            conn.close()


class TableExistsTarget(luigi.Target):
    def __init__(self, db_path, table_name):
        self.db_path = db_path
        self.table_name = table_name

    def exists(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Check if the table exists in the database
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (self.table_name,))
        result = cursor.fetchone()

        conn.close()

        return result is not None
