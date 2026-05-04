import os
import io
import json
import geopandas as gpd
from shapely.geometry import Point
from minio import Minio
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

load_dotenv("/home/keima/projects/flood-pipeline/.env")

MINIO_CLIENT = Minio(
    os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False,
)
BUCKET = "raw-flood-events"

DB_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DB_URL)

def list_geojson_files() -> list:
    objects = MINIO_CLIENT.list_objects(BUCKET, recursive=True)
    return [obj.object_name for obj in objects if obj.object_name.endswith(".geojson")]

def load_geojson(object_name: str) -> list:
    response = MINIO_CLIENT.get_object(BUCKET, object_name)
    data     = json.loads(response.read())
    return data.get("features", [])

def spatial_join_to_kelurahan(features: list) -> list:
    if not features:
        return []

    rows = []
    for f in features:
        coords = f["geometry"]["coordinates"]
        props  = f["properties"]
        rows.append({
            "geometry":     Point(coords[0], coords[1]),
            "pkey":         props.get("pkey"),
            "created_at":   props.get("created_at"),
            "disaster_type": props.get("disaster_type"),
            "severity":     str(props.get("report_data", {}).get("impact", "")),
            "source":       props.get("source", "petabencana"),
        })

    gdf_events = gpd.GeoDataFrame(rows, crs="EPSG:4326")

    # Load kelurahan boundary dari PostGIS
    gdf_kelurahan = gpd.read_postgis(
        "SELECT id, geom FROM kelurahan WHERE geom IS NOT NULL",
        engine, geom_col="geom"
    )

    # Spatial join — point in polygon
    joined = gpd.sjoin(
        gdf_events, gdf_kelurahan,
        how="left", predicate="within"
    )

    # Untuk yang tidak match, cari nearest
    no_match = joined[joined["id"].isna()].copy()
    if not no_match.empty:
        for idx, row in no_match.iterrows():
            distances = gdf_kelurahan.geometry.distance(row.geometry)
            nearest   = gdf_kelurahan.iloc[distances.argmin()]
            joined.at[idx, "id"] = nearest["id"]

    return joined

def upsert_flood_events(joined: gpd.GeoDataFrame):
    with engine.connect() as conn:
        inserted = 0
        for _, row in joined.iterrows():
            if not row.get("id"):
                continue
            try:
                event_date = datetime.fromisoformat(
                    row["created_at"].replace("Z", "+00:00")
                ).date()
            except Exception:
                event_date = None

            conn.execute(text("""
                INSERT INTO flood_events (kelurahan_id, event_date, source, severity, geom)
                VALUES (:kelurahan_id, :event_date, :source, :severity,
                        ST_SetSRID(ST_MakePoint(:lon, :lat), 4326))
                ON CONFLICT DO NOTHING
            """), {
                "kelurahan_id": row["id"],
                "event_date":   event_date,
                "source":       row["source"],
                "severity":     row["severity"],
                "lon":          row["geometry"].x,
                "lat":          row["geometry"].y,
            })
            inserted += 1
        conn.commit()
    return inserted

def run():
    files = list_geojson_files()
    print(f"[*] Found {len(files)} geojson files in MinIO")

    total = 0
    for f in files:
        print(f"    [*] Processing: {f}")
        features = load_geojson(f)
        if not features:
            print(f"    [!] No features, skipping")
            continue

        joined   = spatial_join_to_kelurahan(features)
        inserted = upsert_flood_events(joined)
        total   += inserted
        print(f"    [+] Inserted: {inserted} events")

    print(f"[=] Total flood events loaded: {total}")

if __name__ == "__main__":
    run()

