import os
import io
import json
import geopandas as gpd
import pandas as pd
from minio import Minio
from sqlalchemy import create_engine, text
from shapely.geometry import shape
from dotenv import load_dotenv

load_dotenv("/home/keima/projects/flood-pipeline/.env")

MINIO_CLIENT = Minio(
    os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False,
)

DB_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DB_URL)

def load_rivers_from_minio() -> gpd.GeoDataFrame:
    response = MINIO_CLIENT.get_object("raw-rivers", "rivers_bandung.geojson")
    geojson  = json.loads(response.read())
    gdf      = gpd.GeoDataFrame.from_features(geojson["features"], crs="EPSG:4326")
    return gdf

def load_kelurahan() -> gpd.GeoDataFrame:
    return gpd.read_postgis(
        "SELECT id, nama_kelurahan, geom FROM kelurahan WHERE geom IS NOT NULL",
        engine, geom_col="geom"
    )

def compute_distance_to_river(
    gdf_kelurahan: gpd.GeoDataFrame,
    gdf_rivers: gpd.GeoDataFrame
) -> pd.DataFrame:
    # Reproject ke UTM 48S untuk distance dalam meter
    kel_utm    = gdf_kelurahan.to_crs("EPSG:32748")
    rivers_utm = gdf_rivers.to_crs("EPSG:32748")

    # Gabungkan semua river jadi satu geometry
    all_rivers = rivers_utm.geometry.union_all()

    results = []
    for _, row in kel_utm.iterrows():
        centroid = row["geom"].centroid
        dist_m   = centroid.distance(all_rivers)
        results.append({
            "kelurahan_id":      row["id"],
            "dist_to_river_m":   round(dist_m, 2),
        })

    return pd.DataFrame(results)

def upsert_to_postgis(df: pd.DataFrame):
    with engine.connect() as conn:
        # Tambah kolom kalau belum ada
        conn.execute(text("""
            ALTER TABLE elevation_stats
            ADD COLUMN IF NOT EXISTS dist_to_river_m FLOAT
        """))
        conn.commit()

        for _, row in df.iterrows():
            conn.execute(text("""
                UPDATE elevation_stats
                SET dist_to_river_m = :dist
                WHERE kelurahan_id = :kelurahan_id
            """), {
                "kelurahan_id": row["kelurahan_id"],
                "dist":         row["dist_to_river_m"],
            })
        conn.commit()

def run():
    print("[*] Loading rivers from MinIO...")
    gdf_rivers = load_rivers_from_minio()
    print(f"    Rivers: {len(gdf_rivers)}")

    print("[*] Loading kelurahan from PostGIS...")
    gdf_kelurahan = load_kelurahan()
    print(f"    Kelurahan: {len(gdf_kelurahan)}")

    print("[*] Computing distance to nearest river...")
    df_dist = compute_distance_to_river(gdf_kelurahan, gdf_rivers)

    print("[*] Upserting to PostGIS...")
    upsert_to_postgis(df_dist)

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                ROUND(AVG(dist_to_river_m)::numeric, 2) as avg_dist,
                ROUND(MIN(dist_to_river_m)::numeric, 2) as min_dist,
                ROUND(MAX(dist_to_river_m)::numeric, 2) as max_dist
            FROM elevation_stats
            WHERE dist_to_river_m IS NOT NULL
        """)).fetchone()
        print(f"[=] Avg distance to river: {result[0]} m")
        print(f"    Min: {result[1]} m | Max: {result[2]} m")

if __name__ == "__main__":
    run()
