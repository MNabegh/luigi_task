import luigi

from tasks.data_transformation_task import DataTransformationTask


class LuigiWorkflow(luigi.WrapperTask):
    url_to_json_file = luigi.Parameter()
    json_path = luigi.Parameter()
    staging_path = luigi.Parameter()
    transformation_path = luigi.Parameter()

    def requires(self):
        return DataTransformationTask(
            self.url_to_json_file,
            self.json_path,
            self.staging_path,
            self.transformation_path
        )
