import requests
from google.cloud import bigquery
import os
from flask import Flask, request, jsonify

def places_function(request):
    # Environment variables
    API_KEY = os.environ.get("PLACES_API_KEY")
    BQ_PROJECT = os.environ.get("GCP_PROJECT")
    BQ_DATASET = os.environ.get("BQ_DATASET", "places_dataset")
    BQ_TABLE = os.environ.get("BQ_TABLE", "places")

    # Function to fetch vegan restaurants near Union Square using new Places API
    def fetch_places():
        url = f"https://places.googleapis.com/v1/places:searchText?key={API_KEY}"
        headers = {
            "Content-Type": "application/json",
            "X-Goog-FieldMask": "places.displayName,places.formattedAddress,places.id"
        }
        payload = {
            "textQuery": "vegan restaurants near Union Square, NYC"
        }
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        results = response.json().get("places", [])
        return results[:10]  # Only first 10

    # Function to insert results into BigQuery
    def insert_into_bigquery(rows):
        client = bigquery.Client(project=BQ_PROJECT)
        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
        formatted = []
        for place in rows:
            formatted.append({
                "name": place["displayName"]["text"],
                "address": place.get("formattedAddress"),
                "place_id": place.get("id")
            })
        errors = client.insert_rows_json(table_id, formatted)
        if errors:
            print("BigQuery insert errors:", errors)
        else:
            print(f"Inserted {len(formatted)} rows.")

    # Main execution
    try:
        data = fetch_places()
        insert_into_bigquery(data)
        return jsonify({"status": "success", "inserted": len(data)})
    except Exception as e:
        print("Error:", e)
        return jsonify({"status": "error", "message": str(e)}), 500
