import os
import json
import pandas as pd
import concurrent.futures  # Multi-threading
import time
from flask import Flask, request, jsonify
from flask_cors import CORS
from google.cloud import bigquery
from google.oauth2 import service_account

# Initialize Flask app
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # Allow all origins for local testing

# ✅ Secure authentication using environment variable
PROJECT_ID = "automatic-spotify-scraper"

# Load Google credentials
credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

if credentials_json:
    try:
        credentials_dict = json.loads(credentials_json)
        temp_credentials_path = "/tmp/gcloud-credentials.json"
        with open(temp_credentials_path, "w") as f:
            json.dump(credentials_dict, f)

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_credentials_path
        credentials = service_account.Credentials.from_service_account_file(temp_credentials_path)
        client = bigquery.Client(credentials=credentials)
    except json.JSONDecodeError as e:
        print(f"❌ JSON Decode Error: {e}")
        credentials = None
        client = None
else:
    print("❌ No credentials found in environment variable!")
    credentials = None
    client = None

# ✅ List of datasets
DATASET_LIST = [
    "keywords_ranking_data_sheet1",
    "keywords_ranking_data_sheet2",
    "keywords_ranking_data_sheet3",
    "keywords_ranking_data_sheet4"
]

@app.route('/get_all_tables', methods=['GET'])
def get_all_tables():
    """
    Fetches all unique table names from the datasets in a balanced way.
    Example call: /get_all_tables
    """
    try:
        all_tables = set()
        errors = {}

        def fetch_dataset_tables(dataset):
            """Fetch table names for a single dataset"""
            try:
                query = f"SELECT table_name FROM `{PROJECT_ID}.{dataset}.INFORMATION_SCHEMA.TABLES`"
                query_job = client.query(query)
                return [row.table_name for row in query_job.result()]
            except Exception as e:
                errors[dataset] = str(e)
                print(f"⚠️ Error fetching tables from `{dataset}`: {e}")
                return []

        # ✅ Use a small thread pool to avoid excessive queries
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_to_dataset = {executor.submit(fetch_dataset_tables, dataset): dataset for dataset in DATASET_LIST}
            
            for future in concurrent.futures.as_completed(future_to_dataset):
                dataset_name = future_to_dataset[future]
                try:
                    all_tables.update(future.result())
                except Exception as e:
                    print(f"⚠️ Error processing `{dataset_name}`: {e}")
                    errors[dataset_name] = str(e)

        if errors:
            return jsonify({"status": "error", "message": "Some datasets failed", "errors": errors})

        return jsonify({"status": "success", "tables": list(all_tables)})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

def fetch_table_data(dataset_name, table_name):
    """Fetches table data from BigQuery while limiting excessive queries."""
    try:
        print(f"📥 Fetching `{table_name}` from `{dataset_name}`...")

        # ✅ Optimize query with pagination & caching
        query = f"""
            SELECT * FROM `{PROJECT_ID}.{dataset_name}.{table_name}`
            LIMIT 500  -- ✅ Reduced limit for faster queries
        """
        job_config = bigquery.QueryJobConfig(use_query_cache=True)
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()

        return {dataset_name: [dict(row) for row in results]}

    except Exception as e:
        print(f"⚠️ Error fetching `{table_name}` from `{dataset_name}`: {e}")
        return {dataset_name: {"error": str(e)}}

@app.route('/get_table_data', methods=['GET'])
def get_table_data():
    """
    Fetches a table from all datasets in parallel but with controlled parallelism.
    Example call: /get_table_data?table_name=some_table
    """
    try:
        table_name = request.args.get("table_name")
        if not table_name:
            return jsonify({"status": "error", "message": "Missing table_name parameter"})

        print(f"🚀 Fetching `{table_name}` from all datasets with controlled concurrency...")

        dataset_tables = {}

        # ✅ Reduce parallel requests to 2 at a time
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            future_to_dataset = {
                executor.submit(fetch_table_data, dataset, table_name): dataset
                for dataset in DATASET_LIST
            }

            for future in concurrent.futures.as_completed(future_to_dataset):
                dataset_name = future_to_dataset[future]
                try:
                    dataset_tables[dataset_name] = future.result()[dataset_name]
                except Exception as e:
                    print(f"⚠️ Error processing `{table_name}` from `{dataset_name}`: {e}")
                    dataset_tables[dataset_name] = {"error": str(e)}

        return jsonify({"status": "success", "table": table_name, "data": dataset_tables})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

# ✅ Add logging to monitor slow queries
@app.before_request
def start_timer():
    request.start_time = time.time()

@app.after_request
def log_request(response):
    duration = time.time() - request.start_time
    print(f"⏳ API Request: {request.path} took {duration:.2f}s")
    return response

if __name__ == '__main__':
    app.run(debug=False, host="0.0.0.0", port=5000, threaded=True)
