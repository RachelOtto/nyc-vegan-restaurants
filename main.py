import requests
from google.cloud import bigquery
import os
from flask import Flask, request

def places_function(request):
    # Environment variables
    API_KEY = os.environ.get("PLACES_API_KEY")
    BQ_PROJECT = os.environ.get("GCP_PROJECT")
    BQ_DATASET = os.environ.get("BQ_DATASET", "places_dataset")
    BQ_TABLE = os.environ.get("BQ_TABLE", "places")

    # Function to fetch vegan restaurants in NYC
    def fetch_places(location="40.7831,-73.9712", radius=5000, place_type="restaurant", keyword="vegan"):
        url = (
            f"https://maps.googleapis.com/maps/api/place/nearbysearch/json"
            f"?location={location}&radius={radius}&type={place_type}&keyword={keyword}&key={API_KEY}"
        )
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("results", [])

    # Function to insert results into BigQuery
    def insert_into_bigquery(rows):
        client = bigquery.Client()
        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
        formatted = []
        for place in rows:
            loc = place["geometry"]["location"]
            formatted.append({
                "name": place.get("name"),
                "address": place.get("vicinity"),
                "place_id": place.get("place_id"),
                "rating": place.get("rating"),
                "types": place.get("types"),
                "lat": loc.get("lat"),
                "lng": loc.get("lng"),
                "price": place.get("price_level")  # NEW field
            })
        errors = client.insert_rows_json(table_id, formatted)
        if errors:
            print("BigQuery insert errors:", errors)
        else:
            print(f"Inserted {len(formatted)} rows.")

    # Main execution
    data = fetch_places()
    insert_into_bigquery(data)
    return "Done", 200
