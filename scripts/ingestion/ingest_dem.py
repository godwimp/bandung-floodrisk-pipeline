import os
import time
import json
import io
import requests
import geopandas as gpd
import numpy as np
from minio import Minio
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv("/home/keima/projects/flood-pipeline/.env")

MINIO_CLIENT = Minio(
    os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False,
)
BUCKET  = "raw-dem"
API_URL = "https://api.opentopodata.org/v1/srtm30m"

DB_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DB_URL)

GRID_SIZE = 3  # 3x3 = 9 sample points per kelurahan

def ensure_bucket():
    if not MINIO_CLIENT.bucket_exists(BUCKET):
        MINIO_CLIENT.make_bucket(BUCKET)
        print(f"[+] Bucket '{BUCKET}' created")

def generate_grid_points(geometry, n: int = GRID_SIZE) -> list:
    """Generate n×n grid points di dalam bounding box geometry."""
    minx, miny, maxx, maxy = geometry.bounds
    xs = np.linspace(minx, maxx, n)
    ys = np.linspace(miny, maxy, n)
    points = []
    for y in ys:
        for x in xs:
            points.append((round(y, 6), round(x, 6)))
    return points

def fetch_elevation_batch(points: list) -> list:
    """Fetch elevation untuk batch max 100 points."""
    locations = "|".join(f"{lat},{lon}" for lat, lon in points)
    resp = requests.get(
        API_URL,
        params={"locations": locations, "interpolation": "bilinear"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if data["status"] != "OK":
        raise ValueError(f"API error: {data}")
    return [r["elevation"] for r in data["results"]]

def compute_slope(elevations: list, n: int = GRID_SIZE) -> float:
    """
    Hitung mean slope dari grid elevasi menggunakan finite difference.
    Slope dalam derajat.
    """
    # Bandung area: 1 arc-second ≈ 30m
    cell_size_m = 30.0
    grid = np.array(elevations).reshape(n, n)
    dy, dx = np.gradient(grid, cell_size_m, cell_size_m)
    slope_rad  = np.arctan(np.sqrt(dx**2 + dy**2))
    slope_deg  = np.degrees(slope_rad)
    return float(np.mean(slope_deg))

def load_kelurahan() -> gpd.GeoDataFrame:
    return gpd.read_postgis(
        "SELECT id, nama_kelurahan, geom FROM kelurahan WHERE geom IS NOT NULL",
        engine, geom_col="geom"
    )

def run():
    ensure_bucket()
    gdf = load_kelurahan()
    print(f"[*] Processing {len(gdf)} kelurahan")

    results = []
    total_points = len(gdf) * GRID_SIZE * GRID_SIZE
    calls_needed = (total_points + 99) // 100
    print(f"[*] Total points: {total_points} | API calls needed: {calls_needed}")

    # Kumpulkan semua points dulu
    all_points  = []
    kelurahan_map = []  # track points milik kelurahan mana
    for _, row in gdf.iterrows():
        pts = generate_grid_points(row["geom"], GRID_SIZE)
        all_points.extend(pts)
        kelurahan_map.append({
            "id":             row["id"],
            "nama_kelurahan": row["nama_kelurahan"],
            "start_idx":      len(all_points) - len(pts),
            "end_idx":        len(all_points),
        })

    # Fetch dalam batch 100
    all_elevations = []
    batches        = [all_points[i:i+100] for i in range(0, len(all_points), 100)]
    print(f"[*] Fetching {len(batches)} batches...")

    for i, batch in enumerate(batches):
        try:
            elevs = fetch_elevation_batch(batch)
            all_elevations.extend(elevs)
            print(f"    [+] Batch {i+1}/{len(batches)}: {len(elevs)} points")
        except Exception as e:
            print(f"    [!] Batch {i+1} failed: {e}")
            all_elevations.extend([None] * len(batch))
        time.sleep(1)  # rate limit: 1 call/second

    # Aggregate per kelurahan
    for k in kelurahan_map:
        elevs = all_elevations[k["start_idx"]:k["end_idx"]]
        elevs = [e for e in elevs if e is not None]
        if not elevs:
            print(f"    [!] No elevation data for {k['nama_kelurahan']}")
            continue

        mean_elev = float(np.mean(elevs))
        min_elev  = float(np.min(elevs))
        max_elev  = float(np.max(elevs))
        slope     = compute_slope(elevs) if len(elevs) == GRID_SIZE**2 else None

        results.append({
            "kelurahan_id": k["id"],
            "nama_kelurahan": k["nama_kelurahan"],
            "mean_elevation": round(mean_elev, 2),
            "min_elevation":  round(min_elev, 2),
            "max_elevation":  round(max_elev, 2),
            "mean_slope":     round(slope, 4) if slope else None,
        })

    # Upload ke MinIO
    raw         = json.dumps(results, indent=2).encode()
    buffer      = io.BytesIO(raw)
    object_name = "elevation_stats_kelurahan_bandung.json"
    MINIO_CLIENT.put_object(
        BUCKET, object_name, buffer,
        length=len(raw),
        content_type="application/json",
    )
    print(f"[+] Uploaded: {BUCKET}/{object_name}")
    print(f"[=] Total kelurahan processed: {len(results)}")

if __name__ == "__main__":
    run()
