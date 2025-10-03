from google.cloud import bigquery

def insert_into_bigquery(rows, project, dataset, table):
    client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{table}"
    formatted = [
        {
            "name": place["displayName"]["text"],
            "address": place.get("formattedAddress"),
            "place_id": place.get("id")
        }
        for place in rows
    ]
    errors = client.insert_rows_json(table_id, formatted)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")
    return len(formatted)