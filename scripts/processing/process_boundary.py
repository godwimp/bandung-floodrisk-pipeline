import os
import io
import json
import geopandas as gpd
from minio import Minio
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from shapely.wkb import dumps, loads

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

def load_from_minio() -> gpd.GeoDataFrame:
    response = MINIO_CLIENT.get_object(BUCKET, "boundary_kelurahan_bandung.geojson")
    raw      = response.read()
    gdf      = gpd.read_file(io.BytesIO(raw))
    return gdf

def process_boundary(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # Pastikan CRS EPSG:4326
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    else:
        gdf = gdf.to_crs("EPSG:4326")

    # Hitung luas dalam km² (reproject ke UTM zone 48S untuk kalkulasi meter)
    gdf_utm      = gdf.to_crs("EPSG:32748")
    gdf["luas_km2"] = gdf_utm.geometry.area / 1_000_000

    # Rename kolom agar match schema
    gdf = gdf.rename(columns={
        "id_kelurahan":  "id",
        "nama_kelurahan": "nama_kelurahan",
        "nama_kecamatan": "nama_kecamatan",
    })

    return gdf[["id", "nama_kelurahan", "nama_kecamatan", "luas_km2", "geometry"]]

def drop_z(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
	gdf = gdf.copy()
	gdf["geometry"] = gpd.GeoSeries.from_wkb(
		gdf["geometry"].apply(lambda g: dumps(g, include_srid=False))
)
	return gdf

def upsert_geometry(gdf: gpd.GeoDataFrame):
    with engine.connect() as conn:
        for _, row in gdf.iterrows():
            geom_wkt = row["geometry"].wkt
            conn.execute(text("""
                UPDATE kelurahan
		SET geom     = ST_Multi(ST_Force2D(ST_GeomFromText(:geom, 4326))),
                    luas_km2 = :luas_km2
                WHERE id = :id
            """), {
                "id":       row["id"],
                "geom":     geom_wkt,
                "luas_km2": row["luas_km2"],
            })
        conn.commit()

def run():
    print("[*] Loading boundary from MinIO...")
    gdf = load_from_minio()
    print(f"    Features: {len(gdf)}, CRS: {gdf.crs}")

    gdf = process_boundary(gdf)
    print(f"[*] Processed. Sample luas_km2: {gdf['luas_km2'].describe()}")

    print("[*] Upserting geometry to PostGIS...")
    upsert_geometry(gdf)

    # Verify
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM kelurahan WHERE geom IS NOT NULL
        """)).fetchone()
        print(f"[=] Kelurahan with geometry: {result[0]}")

if __name__ == "__main__":
    run()
