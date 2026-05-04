import os
import requests
import json
import io
from minio import Minio
from dotenv import load_dotenv

load_dotenv("/home/keima/projects/flood-pipeline/.env")

MINIO_CLIENT = Minio(
    os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False,
)
BUCKET  = "raw-population"
URL     = "https://raw.githubusercontent.com/tryfatur/geojson-bandung/master/3273-kota-bandung-level-kelurahan.json"

def ensure_bucket():
    if not MINIO_CLIENT.bucket_exists(BUCKET):
        MINIO_CLIENT.make_bucket(BUCKET)

def fetch_boundary() -> dict:
    resp = requests.get(URL, timeout=30)
    resp.raise_for_status()
    return resp.json()

def upload_to_minio(data: dict):
    raw         = json.dumps(data).encode()
    buffer      = io.BytesIO(raw)
    object_name = "boundary_kelurahan_bandung.geojson"
    MINIO_CLIENT.put_object(
        BUCKET, object_name, buffer,
        length=len(raw),
        content_type="application/geo+json",
    )
    print(f"[+] Uploaded: {BUCKET}/{object_name}")
    print(f"[=] Total features: {len(data['features'])}")

def run():
    ensure_bucket()
    print("[*] Fetching boundary GeoJSON...")
    data = fetch_boundary()
    upload_to_minio(data)

if __name__ == "__main__":
    run()
