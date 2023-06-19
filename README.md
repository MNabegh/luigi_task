python -m unittest test.json_api_fetch_test
everything was added to PYTHONPATH
luigi --module json_api_task FetchDataFromAPI --local-scheduler --json-url https://jsonplaceholder.typicode.com/posts --output-path data/dump.json