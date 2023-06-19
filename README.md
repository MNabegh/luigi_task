python -m unittest test.json_api_fetch_test
everything was added to PYTHONPATH
luigi --module json_api_task FetchDataFromAPI --local-scheduler --json-url https://jsonplaceholder.typicode.com/posts --output-path data/dump.json

# Luigi Data Pipeline

This is a solution for the demonstration task of building a data pipeline that fetches data from the JSONPlaceholder API,
cleans and transforms it, and stores the processed data in a database. The pipeline should be
implemented using the Luigi framework in Python, ensuring proper engineering practices and
implementing basic data quality checks.

This demonstration project was built and ran locally.


# Prerequistes to run it.

1. Install Luigi framework.
1. Add this directory and the task directory to the `$PYTHONPATH` environment variable.
1. Install python `request` package.
1. Install python `unittest` package to run the tests.

# Tasks

## Task 1
The first task was to:

Create a Luigi Task that fetches data from the JSONPlaceholder API. The task should have the
following properties:
- Use the API endpoint https://jsonplaceholder.typicode.com/posts to retrieve data.
- Fetch the data and save it to a local file (e.g., JSON format).
- Implement basic error handling and retries in case of network failures or other errors.
- Ensure that the task is idempotent, meaning it can be run multiple times without duplicating
data.


Each point was achieved by constructing a Luigi task that:
- Uses python's `request` package to fetch the data from the API.
- Writes the file to a local file using python's `json` package.
- Uses try and except to catch and log network error.
- Has two layers of retrial one native to the `request` package and one through configuring Luigi's scheduler as shown in `luigi.cfg`.
- Task is idempotent as the output file is recorded in the overriden `output` function of Luigi which natively ensures that the task is idempotent.

**Improvements**:
1. Unit tests could be improved by using `luigi.MockTarget` to test writing to a file instead of creating a tmp file.