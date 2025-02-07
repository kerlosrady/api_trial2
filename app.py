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
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "automatic-spotify-scraper.json")

# Authenticate BigQuery client
try:
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    print("✅ BigQuery authentication successful!")
except Exception as e:
    print(f"❌ Failed to authenticate with BigQuery: {e}")
    credentials = None
    client = None

# ✅ List of datasets
DATASET_LIST = [
    "keywords_ranking_data_sheet1",
    "keywords_ranking_data_sheet2",
    "keywords_ranking_data_sheet3",
    "keywords_ranking_data_sheet4"
]

def fetch_table_data_parallel(dataset_name, table_name):
    """Fetches table data from BigQuery using parallel processing."""
    try:
        print(f"📥 Fetching `{table_name}` from `{dataset_name}`...")

        # ✅ Fetch only required columns (replace * with specific columns if known)
        query = f"""
            SELECT * FROM `{PROJECT_ID}.{dataset_name}.{table_name}`
            WHERE TRUE  -- Dummy condition for query optimization
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

        # ✅ Step 1: Fetch from all 4 datasets in parallel
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
