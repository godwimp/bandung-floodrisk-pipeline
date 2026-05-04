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
BUCKET = "raw-population"

DB_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DB_URL)

def load_from_minio() -> pd.DataFrame:
    response = MINIO_CLIENT.get_object(BUCKET, "population_kelurahan_bandung.csv")
    df       = pd.read_csv(io.BytesIO(response.read()))
    return df

def get_latest_population(df: pd.DataFrame) -> pd.DataFrame:
    # Ambil tahun terbaru per kelurahan
    latest = df.sort_values("tahun").groupby("kelurahan_id").last().reset_index()
    return latest

def upsert_kelurahan(df: pd.DataFrame):
    with engine.connect() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO kelurahan (id, nama_kelurahan, nama_kecamatan, population)
                VALUES (:id, :nama_kelurahan, :nama_kecamatan, :population)
                ON CONFLICT (id) DO UPDATE
                SET nama_kelurahan = EXCLUDED.nama_kelurahan,
                    nama_kecamatan = EXCLUDED.nama_kecamatan,
                    population     = EXCLUDED.population
            """), {
                "id":             row["kelurahan_id"],
                "nama_kelurahan": row["nama_kelurahan"],
                "nama_kecamatan": row["nama_kecamatan"],
                "population":     int(row["total_penduduk"]),
            })
        conn.commit()
def run():
    print("[*] Loading population data from MinIO...")
    df     = load_from_minio()
    latest = get_latest_population(df)
    print(f"[*] Kelurahan to upsert: {len(latest)}")

    print("[*] Upserting to PostGIS...")
    upsert_kelurahan(latest)

    # Verify
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM kelurahan")).fetchone()
        print(f"[=] Total kelurahan in PostGIS: {result[0]}")

if __name__ == "__main__":
    run()
