import os
import io
import json
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
BUCKET = "raw-dem"

DB_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DB_URL)

def load_from_minio() -> list:
    response = MINIO_CLIENT.get_object(BUCKET, "elevation_stats_kelurahan_bandung.json")
    return json.loads(response.read())

def upsert_elevation_stats(data: list):
    with engine.connect() as conn:
        for row in data:
            conn.execute(text("""
                INSERT INTO elevation_stats (
                    kelurahan_id, mean_elevation, min_elevation, max_elevation, mean_slope, updated_at
                )
                VALUES (
                    :kelurahan_id, :mean_elevation, :min_elevation, :max_elevation, :mean_slope, NOW()
                )
                ON CONFLICT (kelurahan_id) DO UPDATE
                SET mean_elevation = EXCLUDED.mean_elevation,
                    min_elevation  = EXCLUDED.min_elevation,
                    max_elevation  = EXCLUDED.max_elevation,
                    mean_slope     = EXCLUDED.mean_slope,
                    updated_at     = NOW()
            """), {
                "kelurahan_id":  row["kelurahan_id"],
                "mean_elevation": row["mean_elevation"],
                "min_elevation":  row["min_elevation"],
                "max_elevation":  row["max_elevation"],
                "mean_slope":     row["mean_slope"],
            })
        conn.commit()

def run():
    print("[*] Loading DEM data from MinIO...")
    data = load_from_minio()
    print(f"[*] Records to upsert: {len(data)}")

    upsert_elevation_stats(data)

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total,
                ROUND(AVG(mean_elevation)::numeric, 2) as avg_elevation,
                ROUND(MIN(min_elevation)::numeric, 2)  as min_elevation,
                ROUND(MAX(max_elevation)::numeric, 2)  as max_elevation,
                ROUND(AVG(mean_slope)::numeric, 4)     as avg_slope
            FROM elevation_stats
        """)).fetchone()
        print(f"[=] Total: {result[0]} kelurahan")
        print(f"    Avg elevation : {result[1]} m")
        print(f"    Min elevation : {result[2]} m")
        print(f"    Max elevation : {result[3]} m")
        print(f"    Avg slope     : {result[4]}°")

if __name__ == "__main__":
    run()
