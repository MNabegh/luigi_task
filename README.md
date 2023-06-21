# Luigi Data Pipeline

This is a solution for the demonstration task of building a data pipeline that fetches data from the JSONPlaceholder API,
cleans and transforms it, and stores the processed data in a database. The pipeline should be
implemented using the Luigi framework in Python, ensuring proper engineering practices and
implementing basic data quality checks.

This demonstration project was built and ran locally.


## Prerequistes to run it.

1. Python version >= 3.9
1. Install python required packages using `requirements.txt` by running the command `pip install -r requirements.txt`.
1. Add this directory and the task directory to the `$PYTHONPATH` environment variable by running the command `export PYTHONPATH=$(pwd):$(pwd)/tasks` in the root directory for this project on Linux machines.
1. Create SQL lite database.
1. Create SQL lite table.
1. Create logs directory in the root directory
1. Create data directory in the root directory
1. Create db directory in the root directory

## Workflow

This project has one Luigi workflow that can be run from the root directory of the project to:
1. Fetch JSON data from an API (`json_api_task.py`)
1. Validate the data quality (`data_cleaning_task.py`)
1. Clean the data and write it to stagging layer  (`data_cleaning_task.py`)
1. Transform the data to analytics format  (`data_transformation_task.py`)
1. Load the transformed data into a database (`load_data_into_db_task.py`)

The workflow is in the file `luigi_workflow.py`

To run the workflow use the command `luigi --module luigi_workflow LuigiWorkflow --json-path <json_path_to_save_downloaded_file> --staging-path <parquet_path_to_save_staged_data>  --url-to-json-file https://jsonplaceholder.typicode.com/posts --transformation-path <parquet_path_to_save_transformed_data> --db-connection-string <path_to_connect_to_db> --table-name <db_table_name>`

### Extra tasks
Two Extra tasks were created to create a Database and a Table that could be run independtly, however, they should not be included in the workflow because:
1. It is a bad practice to couple processing tasks with infrastructure management tasks.
1. You can't as Luigi checks the task completion before it checks the required dependency completion which will create circurlar dependency.

## Configurations
There are two configuration files:

1. `luigi.cfg`
    1. It points to the logging configurations.
    1. Configure retrials procedure for failed tasks.
1. `logging.conf`
    1. Configure logging targets
    1. Configure logging levels for each target
    1. Configure logging format

## Unit tests
Unit tests have been implemented for most of the pipeline using the python library `unittest` and they are all written in the directory `test`. However, Unit tests have been skipped for the optional task due to lack of time. To run the tests, you can use the following command `python -m unittest test.<test_module>`.

## Improvements
- Testing of reading and writing files could be improved using mocks such `luigi.MockTarget` but it could not be done due to time constraints.