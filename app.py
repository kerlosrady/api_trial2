import os
import json
import pandas as pd
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
                print(f"🔍 Querying tables from `{dataset}`...")  # Debugging print

                query = f"SELECT table_name FROM `{PROJECT_ID}.{dataset}.INFORMATION_SCHEMA.TABLES`"
                query_job = client.query(query)
                table_names = [row.table_name for row in query_job.result()]

                print(f"✅ Found {len(table_names)} tables in `{dataset}`: {table_names}")  # Debugging print

                all_tables.update(table_names)  # Add unique table names

            except Exception as e:
                errors[dataset] = str(e)
                print(f"⚠️ Error fetching tables from `{dataset}`: {e}")

        if errors:
            return jsonify({"status": "error", "message": "Errors occurred while fetching tables", "errors": errors})

        return jsonify({"status": "success", "tables": list(all_tables)})

    except Exception as e:
        print(f"❌ API Error: {e}")
        return jsonify({"status": "error", "message": str(e)})



@app.route('/get_table_data', methods=['GET'])
def get_table_data():
    """
    Fetches a table from all 4 datasets.
    Example call: /get_table_data?table_name=some_table
    """
    try:
        table_name = request.args.get("table_name")
        if not table_name:
            return jsonify({"status": "error", "message": "Missing table_name parameter"})

        print(f"📥 Fetching table `{table_name}` from all datasets...")

        dataset_tables = {}

        # ✅ Step 1: Fetch from all 4 datasets
        for i, dataset in enumerate(DATASET_LIST, start=1):
            try:
                query = f"SELECT * FROM `{PROJECT_ID}.{dataset}.{table_name}` LIMIT 5000"
                query_job = client.query(query)
                results = query_job.result()

                # ✅ Convert to dictionary format
                data = [dict(row) for row in results]
                dataset_tables[str(i)] = data  # Store data under dataset number ("1", "2", etc.)

            except Exception as e:
                print(f"⚠️ Error fetching `{table_name}` from `{dataset}`: {e}")
                dataset_tables[str(i)] = {"error": str(e)}

        return jsonify({"status": "success", "table": table_name, "data": dataset_tables})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

if __name__ == '__main__':
    app.run(debug=False, host="0.0.0.0", port=5000)  # Production-ready setup
