import luigi

from tasks.create_db_task import CreateDatabaseTask
from tasks.create_table_task import CreateTableTask
from tasks.load_data_into_db_task import LoadDataToDatabaseTask


class LuigiWorkflow(luigi.WrapperTask):
    url_to_json_file = luigi.Parameter()
    json_path = luigi.Parameter()
    staging_path = luigi.Parameter()
    transformation_path = luigi.Parameter()

    def requires(self):
        return LoadDataToDatabaseTask(
            self.url_to_json_file,
            self.json_path,
            self.staging_path,
            self.transformation_path
        )
