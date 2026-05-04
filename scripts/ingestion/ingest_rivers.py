import os
import io
import json
import requests
import time
from minio import Minio
from dotenv import load_dotenv

load_dotenv("/home/keima/projects/flood-pipeline/.env")

MINIO_CLIENT = Minio(
    os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False,
)
BUCKET        = "raw-rivers"
OVERPASS_URL  = "https://overpass-api.de/api/interpreter"
BANDUNG_BBOX  = "(-7.05,107.50,-6.80,107.75)"

def ensure_bucket():
    if not MINIO_CLIENT.bucket_exists(BUCKET):
        MINIO_CLIENT.make_bucket(BUCKET)
        print(f"[+] Bucket '{BUCKET}' created")

def fetch_rivers() -> dict:
	query = f'[out:json];way[waterway~"river|stream"]{BANDUNG_BBOX};out geom;'
	resp = requests.post(
		OVERPASS_URL,
		data=query,
		headers={
			"Content-Type": "application/x-www-form-urlencoded",
			"User-Agent": "flood-pipeline/1.0",
		},
		timeout=60,
	)
	resp.raise_for_status()
	return resp.json()

def to_geojson(osm_data: dict) -> dict:
    features = []
    for element in osm_data.get("elements", []):
        if element["type"] != "way" or "geometry" not in element:
            continue
        coords = [[pt["lon"], pt["lat"]] for pt in element["geometry"]]
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": coords,
            },
            "properties": {
                "osm_id":   element["id"],
                "waterway": element.get("tags", {}).get("waterway", "unknown"),
                "name":     element.get("tags", {}).get("name", None),
            }
        })
    return {
        "type":     "FeatureCollection",
        "features": features,
    }

def upload_to_minio(geojson: dict):
    raw         = json.dumps(geojson).encode()
    buffer      = io.BytesIO(raw)
    object_name = "rivers_bandung.geojson"
    MINIO_CLIENT.put_object(
        BUCKET, object_name, buffer,
        length=len(raw),
        content_type="application/geo+json",
    )
    print(f"[+] Uploaded: {BUCKET}/{object_name}")

def run():
    ensure_bucket()
    print("[*] Fetching river/stream data from Overpass API...")
    osm_data = fetch_rivers()

    total_elements = len(osm_data.get("elements", []))
    print(f"[*] Total OSM elements: {total_elements}")

    geojson = to_geojson(osm_data)
    print(f"[*] Total features: {len(geojson['features'])}")

    upload_to_minio(geojson)
    print("[=] Done")

if __name__ == "__main__":
    run()
