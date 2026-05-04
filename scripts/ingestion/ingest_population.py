import os
import requests
import pandas as pd
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
API_URL = "https://opendata.bandung.go.id/api/bigdata/dinas_kependudukan_dan_pencatatan_sipil/jumlah_penduduk_kota_bandung_berdasarkan_jenis_kelamin"

def ensure_bucket():
    if not MINIO_CLIENT.bucket_exists(BUCKET):
        MINIO_CLIENT.make_bucket(BUCKET)
        print(f"[+] Bucket '{BUCKET}' created")

def fetch_all_pages() -> list:
    all_data = []
    page     = 1
    while True:
        resp = requests.get(API_URL, params={"limit": 100, "page": page}, timeout=30)
        resp.raise_for_status()
        rows = resp.json()["data"]
        if not rows:
            break
        all_data.extend(rows)
        print(f"    [+] Page {page}: {len(rows)} rows")
        page += 1
    return all_data

def aggregate_population(data: list) -> pd.DataFrame:
    df = pd.DataFrame(data)
    # Aggregate L + P per kelurahan
    agg = df.groupby([
        "bps_kode_desa_kelurahan",
        "bps_desa_kelurahan",
        "bps_kode_kecamatan",
        "bps_nama_kecamatan",
        "tahun",
    ])["jumlah_penduduk"].sum().reset_index()
    agg.columns = [
        "kelurahan_id",
        "nama_kelurahan",
        "kecamatan_id",
        "nama_kecamatan",
        "tahun",
        "total_penduduk",
    ]
    return agg

def upload_to_minio(df: pd.DataFrame):
    csv_buffer  = io.BytesIO(df.to_csv(index=False).encode())
    object_name = f"population_kelurahan_bandung.csv"
    MINIO_CLIENT.put_object(
        BUCKET, object_name, csv_buffer,
        length=csv_buffer.getbuffer().nbytes,
        content_type="text/csv",
    )
    print(f"[+] Uploaded: {BUCKET}/{object_name} ({len(df)} kelurahan)")

def run():
    ensure_bucket()
    print("[*] Fetching population data...")
    raw_data = fetch_all_pages()
    print(f"[*] Total raw rows: {len(raw_data)}")

    df = aggregate_population(raw_data)
    print(f"[*] Total kelurahan: {len(df)}")
    print(df.head())

    upload_to_minio(df)
    print("[=] Done")

if __name__ == "__main__":
    run()
