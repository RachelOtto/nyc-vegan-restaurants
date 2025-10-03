import os
from flask import jsonify
from places_api import fetch_places
from bigquery_utils import insert_into_bigquery

def places_function(request):
    try:
        API_KEY = os.environ["PLACES_API_KEY"]
        PROJECT = os.environ["GCP_PROJECT"]
        DATASET = os.environ["BQ_DATASET"]
        TABLE = os.environ["BQ_TABLE"]

        places = fetch_places(API_KEY)
        inserted_count = insert_into_bigquery(places, PROJECT, DATASET, TABLE)

        return jsonify({"status": "success", "inserted": inserted_count})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500