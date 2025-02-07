import os
import json
import pandas as pd
import concurrent.futures
from flask import Flask, Response, request, jsonify
from flask_cors import CORS
from google.cloud import bigquery
from google.oauth2 import service_account

# Initialize Flask app
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # Allow all origins for local testing

# ✅ Secure authentication using environment variable
PROJECT_ID = "automatic-spotify-scraper"
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "automatic-spotify-scraper.json")

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

def stream_table_data(dataset_name, table_name):
    """Streams BigQuery data row-by-row to reduce memory usage."""
    try:
        print(f"📥 Streaming data from `{PROJECT_ID}.{dataset_name}.{table_name}`...")

        query = f"SELECT * FROM `{PROJECT_ID}.{dataset_name}.{table_name}` LIMIT 5000"  # Limit to prevent overload
        query_job = client.query(query)

        def generate():
            yield '{"status": "success", "data": ['  # Open JSON array
            first_row = True
            for row in query_job.result():
                if not first_row:
                    yield ','
                yield json.dumps(dict(row))  # Convert row to JSON
                first_row = False
            yield ']}'  # Close JSON array

        return Response(generate(), content_type="application/json")

    except Exception as e:
        print(f"⚠️ Error streaming `{dataset_name}.{table_name}`: {e}")
        return jsonify({"status": "error", "message": str(e)})
@app.route('/get_all_tables_data', methods=['GET'])
def get_all_tables_data():
    try:
        dataset_list = [
            "keywords_ranking_data_sheet1",
            "keywords_ranking_data_sheet2",
            "keywords_ranking_data_sheet3",
            "keywords_ranking_data_sheet4"
        ]

        full_response = {"status": "success", "data": {}}

        for dataset_name in dataset_list:
            full_response["data"][dataset_name] = {}

            query = f"SELECT table_name FROM `{PROJECT_ID}.{dataset_name}.INFORMATION_SCHEMA.TABLES`"
            query_job = client.query(query)
            table_names = [row.table_name for row in query_job.result()]

            for table_name in table_names:
                query_data = f"SELECT * FROM `{PROJECT_ID}.{dataset_name}.{table_name}` LIMIT 200"
                results = client.query(query_data).result()
                table_data = [dict(row) for row in results]
                full_response["data"][dataset_name][table_name] = table_data

        json_response = json.dumps(full_response, indent=2)
        print(f"✅ API Response Size: {len(json_response)} characters")  # Debug print

        return Response(json_response, content_type="application/json")

    except Exception as e:
        print(f"❌ API Error: {e}")
        return jsonify({"status": "error", "message": str(e)})

if __name__ == '__main__':
    app.run(debug=False, host="0.0.0.0", port=5000, threaded=True)  # Multi-threading enabled
