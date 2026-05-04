import os
import io
import pandas as pd
from minio import Minio
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv("/home/keima/projects/flood-pipeline/.env")

MINIO_CLIENT = Minio(
    os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False,
)
BUCKET = "raw-rainfall"

DB_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DB_URL)

# Mapping station ke kelurahan_id sementara (sebelum ada boundary data)
STATION_KELURAHAN_MAP = {
    "bandung_pusat":   "BDG-PUSAT",
    "bandung_utara":   "BDG-UTARA",
    "bandung_selatan": "BDG-SELATAN",
    "bandung_timur":   "BDG-TIMUR",
    "bandung_barat":   "BDG-BARAT",
}

def list_objects(bucket: str) -> list:
    objects = MINIO_CLIENT.list_objects(bucket, recursive=True)
    return [obj.object_name for obj in objects]

def load_csv_from_minio(object_name: str) -> pd.DataFrame:
    response = MINIO_CLIENT.get_object(BUCKET, object_name)
    df = pd.read_csv(io.BytesIO(response.read()))
    return df

def upsert_to_postgis(df: pd.DataFrame):
    with engine.connect() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO rainfall_daily (kelurahan_id, date, precipitation, rain_sum, source)
                VALUES (:kelurahan_id, :date, :precipitation, :rain_sum, :source)
                ON CONFLICT (kelurahan_id, date, source) DO UPDATE
                SET precipitation = EXCLUDED.precipitation,
                    rain_sum      = EXCLUDED.rain_sum
            """), {
                "kelurahan_id": STATION_KELURAHAN_MAP.get(row["station"], row["station"]),
                "date":         row["date"],
                "precipitation": row["precipitation"],
                "rain_sum":     row["rain_sum"],
                "source":       "open_meteo",
            })
        conn.commit()

def run():
    objects = list_objects(BUCKET)
    print(f"[*] Found {len(objects)} files in MinIO")

    total_rows = 0
    for obj in objects:
        try:
            df = load_csv_from_minio(obj)
            upsert_to_postgis(df)
            total_rows += len(df)
            print(f"    [+] Loaded: {obj} ({len(df)} rows)")
        except Exception as e:
            print(f"    [!] Failed {obj}: {e}")

    print(f"\n[=] Total rows loaded: {total_rows}")

if __name__ == "__main__":
    run()

