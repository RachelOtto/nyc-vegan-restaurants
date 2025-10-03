import requests

def fetch_places(api_key, query="vegan restaurants near Union Square, NYC"):
    url = f"https://places.googleapis.com/v1/places:searchText?key={api_key}"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-FieldMask": "places.displayName,places.formattedAddress,places.id"
    }
    payload = {"textQuery": query}
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.json().get("places", [])[:10]
