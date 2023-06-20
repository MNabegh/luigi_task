import luigi
import sqlite3


class CreateDatabaseTask(luigi.Task):
    db_path = luigi.Parameter(default="db/example.db")

    def run(self):
        # Connect to a temporary database
        conn = sqlite3.connect(self.db_path)
        conn.close()

    def output(self):
        # Output target to mark the task as complete
        return luigi.LocalTarget(self.db_path)
