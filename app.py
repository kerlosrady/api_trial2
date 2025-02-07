# import os
# import json
# import pandas as pd
# from flask import Flask, request, jsonify
# from flask_cors import CORS
# from google.cloud import bigquery
# from google.oauth2 import service_account
#
# # Initialize Flask app
# app = Flask(__name__)
# CORS(app, resources={r"/*": {"origins": "*"}})  # Allow all origins for local testing
#
# # ✅ Secure authentication using environment variable
# PROJECT_ID = "automatic-spotify-scraper"  # Ensure this is the correct project
# SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "automatic-spotify-scraper.json")
#
# # Authenticate BigQuery client
# try:
#     credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
#     client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
#     print("✅ BigQuery authentication successful!")
# except Exception as e:
#     print(f"❌ Failed to authenticate with BigQuery: {e}")
#     credentials = None
#     client = None
#
#
# @app.route('/get_all_tables_data', methods=['GET'])
# def get_all_tables_data():
#     try:
#         dataset_list = [
#             "keywords_ranking_data_sheet1",
#             # "keywords_ranking_data_sheet2",
#             # "keywords_ranking_data_sheet3",
#             # "keywords_ranking_data_sheet4"
#         ]
#
#         dataset_tables = {}
#
#         # ✅ Step 1: Fetch all table names from each dataset's `INFORMATION_SCHEMA`
#         all_tables = []
#         for dataset in dataset_list:
#             query = f"""
#                 SELECT '{dataset}' AS dataset_name, table_name
#                 FROM `{PROJECT_ID}.{dataset}.INFORMATION_SCHEMA.TABLES`
#             """
#
#             query_job = client.query(query)
#             results = query_job.result()
#             for row in results:
#                 all_tables.append((row.dataset_name, row.table_name))
#
#         if not all_tables:
#             return jsonify({"status": "error", "message": "No tables found in datasets."})
#
#         # ✅ Step 2: Iterate through all datasets and tables, fetch data
#         for dataset_name, table_name in all_tables:
#             dataset_key = dataset_name.replace('keywords_ranking_data_sheet', '')
#
#             if dataset_key not in dataset_tables:
#                 dataset_tables[dataset_key] = {}
#
#             print(f"📥 Fetching data from {dataset_name}.{table_name}")
#
#             # ✅ Fetch data (LIMIT to prevent memory overload)
#             table_query = f"SELECT * FROM `{PROJECT_ID}.{dataset_name}.{table_name}` LIMIT 10000"
#
#             try:
#                 table_job = client.query(table_query)
#                 table_results = table_job.result()
#                 data = [dict(record) for record in table_results]
#                 dataset_tables[dataset_key][table_name] = data
#
#             except Exception as e:
#                 print(f"⚠️ Error fetching {dataset_name}.{table_name}: {e}")
#                 dataset_tables[dataset_key][table_name] = {"error": str(e)}
#
#         return jsonify({"status": "success", "data": dataset_tables})
#
#     except Exception as e:
#         return jsonify({"status": "error", "message": str(e)})
#
#
# if __name__ == '__main__':
#     app.run(debug=False, host="0.0.0.0", port=5000)  # Production-ready setup



import os
import json
import pandas as pd
import concurrent.futures  # Multi-threading
import multiprocessing  # Multi-processing
from flask import Flask, request, jsonify
from flask_cors import CORS
from google.cloud import bigquery
from google.oauth2 import service_account

# Initialize Flask app
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # Allow all origins for local testing

# ✅ Secure authentication using environment variable
PROJECT_ID = "automatic-spotify-scraper"  # Ensure this is the correct project
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


def fetch_table_data(dataset_name, table_name):
    """Fetches data from a given table efficiently using multiprocessing."""
    try:
        print(f"📥 Fetching data from `{PROJECT_ID}.{dataset_name}.{table_name}`...")

        # ✅ Fetch only needed columns (replace `*` with required columns if possible)
        query = f"SELECT * FROM `{PROJECT_ID}.{dataset_name}.{table_name}` LIMIT 10000"
        query_job = client.query(query)
        results = query_job.result()

        data = [dict(row) for row in results]
        print(f"✅ Retrieved {len(data)} rows from {dataset_name}.{table_name}")

        return {table_name: data}

    except Exception as e:
        print(f"⚠️ Error fetching `{PROJECT_ID}.{dataset_name}.{table_name}`: {e}")
        return {table_name: {"error": str(e)}}


@app.route('/get_all_tables_data', methods=['GET'])
def get_all_tables_data():
    try:
        dataset_list = [
            "keywords_ranking_data_sheet1",
            "keywords_ranking_data_sheet2",
            "keywords_ranking_data_sheet3",
            "keywords_ranking_data_sheet4"
        ]

        dataset_tables = {}

        # ✅ Step 1: Fetch all table names from each dataset's `INFORMATION_SCHEMA`
        all_tables = []
        for dataset in dataset_list:
            query = f"""
                SELECT '{dataset}' AS dataset_name, table_name
                FROM `{PROJECT_ID}.{dataset}.INFORMATION_SCHEMA.TABLES`
            """

            query_job = client.query(query)
            results = query_job.result()
            for row in results:
                all_tables.append((row.dataset_name, row.table_name))

        if not all_tables:
            return jsonify({"status": "error", "message": "No tables found in datasets."})

        # ✅ Step 2: Fetch data in Parallel using Multi-Threading
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_table = {
                executor.submit(fetch_table_data, dataset_name, table_name): (dataset_name, table_name)
                for dataset_name, table_name in all_tables
            }

            for future in concurrent.futures.as_completed(future_to_table):
                dataset_name, table_name = future_to_table[future]
                dataset_key = dataset_name.replace('keywords_ranking_data_sheet', '')

                if dataset_key not in dataset_tables:
                    dataset_tables[dataset_key] = {}

                try:
                    table_data = future.result()
                    dataset_tables[dataset_key].update(table_data)
                except Exception as e:
                    print(f"⚠️ Error processing `{dataset_name}.{table_name}`: {e}")
                    dataset_tables[dataset_key][table_name] = {"error": str(e)}

        return jsonify({"status": "success", "data": dataset_tables})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

if __name__ == '__main__':
    app.run(debug=False, host="0.0.0.0", port=5000, threaded=True)  # Multi-threading enabled
