import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from minio import Minio
from dotenv import load_dotenv
import io

load_dotenv("/home/keima/projects/flood-pipeline/.env")

MINIO_CLIENT = Minio(
    os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False,
)
BUCKET = "raw-rainfall"

STATIONS = [
    {"name": "bandung_pusat",   "lat": -6.9175, "lon": 107.6191},
    {"name": "bandung_utara",   "lat": -6.8615, "lon": 107.6113},
    {"name": "bandung_selatan", "lat": -7.0051, "lon": 107.6386},
    {"name": "bandung_timur",   "lat": -6.9400, "lon": 107.7050},
    {"name": "bandung_barat",   "lat": -6.9100, "lon": 107.5340},
]

# Gunakan historical endpoint untuk data lama, forecast untuk 92 hari terakhir
HISTORICAL_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
FORECAST_URL   = "https://api.open-meteo.com/v1/forecast"

def ensure_bucket():
    if not MINIO_CLIENT.bucket_exists(BUCKET):
        MINIO_CLIENT.make_bucket(BUCKET)
        print(f"[+] Bucket '{BUCKET}' created")

def date_chunks(start: str, end: str, chunk_days: int = 90):
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt   = datetime.strptime(end,   "%Y-%m-%d")
    chunks   = []
    current  = start_dt
    while current < end_dt:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_dt)
        chunks.append((current.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        current = chunk_end + timedelta(days=1)
    return chunks

def fetch_chunk(station: dict, start_date: str, end_date: str) -> pd.DataFrame:
    today    = datetime.now().date()
    end_dt   = datetime.strptime(end_date, "%Y-%m-%d").date()
    days_ago = (today - end_dt).days

    # Pilih endpoint berdasarkan umur data
    if days_ago > 5:
        url = HISTORICAL_URL
        params = {
            "latitude":    station["lat"],
            "longitude":   station["lon"],
            "start_date":  start_date,
            "end_date":    end_date,
            "daily":       "precipitation_sum,rain_sum",
            "timezone":    "Asia/Jakarta",
        }
    else:
        url = FORECAST_URL
        params = {
            "latitude":      station["lat"],
            "longitude":     station["lon"],
            "daily":         "precipitation_sum,rain_sum",
            "timezone":      "Asia/Jakarta",
            "past_days":     min((today - datetime.strptime(start_date, "%Y-%m-%d").date()).days, 92),
            "forecast_days": 1,
        }

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    df = pd.DataFrame({
        "date":          data["daily"]["time"],
        "precipitation": data["daily"]["precipitation_sum"],
        "rain_sum":      data["daily"].get("rain_sum", [None] * len(data["daily"]["time"])),
        "station":       station["name"],
        "lat":           station["lat"],
        "lon":           station["lon"],
    })
    df["date"] = pd.to_datetime(df["date"])
    df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
    return df

def upload_to_minio(df: pd.DataFrame, station_name: str, start_date: str, end_date: str):
    csv_buffer  = io.BytesIO(df.to_csv(index=False).encode())
    year        = start_date[:4]
    object_name = f"{year}/{station_name}_{start_date}_{end_date}.csv"
    MINIO_CLIENT.put_object(
        BUCKET, object_name, csv_buffer,
        length=csv_buffer.getbuffer().nbytes,
        content_type="text/csv",
    )
    print(f"    [+] Uploaded: {object_name} ({len(df)} rows)")

def run(start_date: str = None, end_date: str = None):
    if not end_date:
        end_date   = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    if not start_date:
        start_date = (datetime.now() - timedelta(days=92)).strftime("%Y-%m-%d")

    ensure_bucket()
    chunks = date_chunks(start_date, end_date, chunk_days=90)
    print(f"[*] {len(chunks)} chunks | {start_date} to {end_date}")

    for station in STATIONS:
        print(f"\n[*] Station: {station['name']}")
        total_rows = 0
        for chunk_start, chunk_end in chunks:
            try:
                df = fetch_chunk(station, chunk_start, chunk_end)
                if not df.empty:
                    upload_to_minio(df, station["name"], chunk_start, chunk_end)
                    total_rows += len(df)
            except Exception as e:
                print(f"    [!] Failed {chunk_start}~{chunk_end}: {e}")
        print(f"    [=] Total: {total_rows} rows")

if __name__ == "__main__":
    run(start_date="2020-01-01")
