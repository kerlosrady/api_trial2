import os
import json
import pandas as pd
import concurrent.futures  # Multi-threading
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
    Fetches all unique table names from the datasets.
    Example call: /get_all_tables
    """
    try:
        all_tables = set()
        errors = {}

        for dataset in DATASET_LIST:
            try:
                print(f"🔍 Querying tables from `{dataset}`...")
                query = f"SELECT table_name FROM `{PROJECT_ID}.{dataset}.INFORMATION_SCHEMA.TABLES`"
                query_job = client.query(query)
                table_names = [row.table_name for row in query_job.result()]
                all_tables.update(table_names)
            except Exception as e:
                errors[dataset] = str(e)
                print(f"⚠️ Error fetching tables from `{dataset}`: {e}")

        if errors:
            return jsonify({"status": "error", "message": "Some datasets failed", "errors": errors})

        return jsonify({"status": "success", "tables": list(all_tables)})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

def fetch_table_data_parallel(dataset_name, table_name):
    """Fetches table data from BigQuery using parallel processing."""
    try:
        print(f"📥 Fetching `{table_name}` from `{dataset_name}`...")

        # ✅ Fetch only required columns (replace * with specific columns if known)
        query = f"""
            SELECT * FROM `{PROJECT_ID}.{dataset_name}.{table_name}`
            LIMIT 1000
        """
        job_config = bigquery.QueryJobConfig(use_query_cache=True)  # ✅ Enable Query Caching
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()

        # ✅ Convert to dictionary format
        return {dataset_name: [dict(row) for row in results]}

    except Exception as e:
        print(f"⚠️ Error fetching `{table_name}` from `{dataset_name}`: {e}")
        return {dataset_name: {"error": str(e)}}

@app.route('/get_table_data', methods=['GET'])
def get_table_data():
    """
    Fetches a table from all 4 datasets in parallel.
    Example call: /get_table_data?table_name=some_table
    """
    try:
        table_name = request.args.get("table_name")
        if not table_name:
            return jsonify({"status": "error", "message": "Missing table_name parameter"})

        print(f"🚀 Fetching `{table_name}` from all datasets in parallel...")

        dataset_tables = {}

        # ✅ Fetch from all 4 datasets in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            future_to_dataset = {
                executor.submit(fetch_table_data_parallel, dataset, table_name): dataset
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

if __name__ == '__main__':
    app.run(debug=False, host="0.0.0.0", port=5000, threaded=True)  # Multi-threading enabled
