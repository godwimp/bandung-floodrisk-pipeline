import os
import requests
import json
import io
from datetime import datetime
from minio import Minio
from dotenv import load_dotenv

load_dotenv("/home/keima/projects/flood-pipeline/.env")

MINIO_CLIENT = Minio(
    os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False,
)
BUCKET      = "raw-flood-events"
API_URL     = "https://data.petabencana.id/reports"
TIME_PERIOD = 2592000  # 30 hari dalam detik

BANDUNG_REGION_PREFIX = "3273"
BANDUNG_PROVINCE_CODE = "ID-JB"

def ensure_bucket():
    if not MINIO_CLIENT.bucket_exists(BUCKET):
        MINIO_CLIENT.make_bucket(BUCKET)
        print(f"[+] Bucket '{BUCKET}' created")

def fetch_reports(timeperiod: int) -> list:
    resp = requests.get(API_URL, params={"timeperiod": timeperiod}, timeout=30)
    resp.raise_for_status()
    data       = resp.json()
    geometries = data["result"]["objects"]["output"]["geometries"]
    return geometries

def filter_bandung_floods(geometries: list) -> list:
    result = []
    for g in geometries:
        tags          = g["properties"]["tags"]
        region_code   = (tags.get("region_code") or "")
        instance_code = (tags.get("instance_region_code") or "")
        is_bandung    = region_code.startswith(BANDUNG_REGION_PREFIX) or instance_code == BANDUNG_PROVINCE_CODE
        is_flood      = g["properties"]["disaster_type"] == "flood"
        if is_bandung and is_flood:
            result.append(g)
    return result

def upload_to_minio(data: list):
    today       = datetime.now().strftime("%Y-%m-%d")
    object_name = f"{today[:4]}/flood_events_{today}.geojson"
    geojson     = {
        "type":     "FeatureCollection",
        "fetched_at": datetime.now().isoformat(),
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type":        "Point",
                    "coordinates": g["coordinates"],
                },
                "properties": g["properties"],
            }
            for g in data
        ],
    }
    raw    = json.dumps(geojson, indent=2).encode()
    buffer = io.BytesIO(raw)
    MINIO_CLIENT.put_object(
        BUCKET, object_name, buffer,
        length=len(raw),
        content_type="application/geo+json",
    )
    print(f"[+] Uploaded: {BUCKET}/{object_name}")
    return geojson

def run(timeperiod: int = TIME_PERIOD):
    ensure_bucket()
    print(f"[*] Fetching reports — last {timeperiod // 86400} days...")

    all_reports     = fetch_reports(timeperiod)
    bandung_floods  = filter_bandung_floods(all_reports)

    print(f"    Total reports : {len(all_reports)}")
    print(f"    Bandung floods: {len(bandung_floods)}")

    if bandung_floods:
        upload_to_minio(bandung_floods)
    else:
        print("[!] No flood events found for Bandung — skipping upload")

    print(f"[=] Done")

if __name__ == "__main__":
    run()

