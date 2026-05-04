import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import date

load_dotenv("/home/keima/projects/flood-pipeline/.env")

DB_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DB_URL)

def minmax(series: pd.Series) -> pd.Series:
    mn, mx = series.min(), series.max()
    if mx == mn:
        return pd.Series(0.5, index=series.index)
    return (series - mn) / (mx - mn)

def load_features() -> pd.DataFrame:
    query = """
        SELECT
            k.id                                        AS kelurahan_id,
            k.nama_kelurahan,
            k.nama_kecamatan,
            k.population,
            k.luas_km2,
            COALESCE(k.population, 0) / NULLIF(k.luas_km2, 0) AS pop_density,
            e.mean_elevation,
            e.mean_slope,
            e.dist_to_river_m,
            COALESCE(
                (SELECT SUM(r.precipitation)
                 FROM rainfall_daily r
                 WHERE r.kelurahan_id = k.id
                   AND r.date >= CURRENT_DATE - INTERVAL '7 days'),
                0
            )                                           AS rainfall_7day,
            COALESCE(
                (SELECT COUNT(*)
                 FROM flood_events f
                 WHERE f.kelurahan_id = k.id),
                0
            )                                           AS flood_frequency
        FROM kelurahan k
        LEFT JOIN elevation_stats e ON e.kelurahan_id = k.id
        WHERE k.id NOT IN ('BDG-PUSAT','BDG-UTARA','BDG-SELATAN','BDG-TIMUR','BDG-BARAT')
          AND k.luas_km2 IS NOT NULL
          AND e.mean_elevation IS NOT NULL
    """
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)

def compute_fri(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Normalisasi
    df["elev_norm_inv"]  = 1 - minmax(df["mean_elevation"])
    df["slope_norm_inv"] = 1 - minmax(df["mean_slope"])
    df["rain_norm"]      = minmax(df["rainfall_7day"])
    df["flood_norm"]     = minmax(df["flood_frequency"])
    df["dist_norm_inv"]  = 1 - minmax(df["dist_to_river_m"])
    df["pop_norm"]       = minmax(df["pop_density"])
    df["area_norm_inv"]  = 1 - minmax(df["luas_km2"])

    # Hazard score
    df["H"] = (
        0.25 * df["elev_norm_inv"] +
        0.20 * df["slope_norm_inv"] +
        0.20 * df["rain_norm"] +
        0.20 * df["flood_norm"] +
        0.15 * df["dist_norm_inv"]
    )

    # Vulnerability score
    df["V"] = (
        0.70 * df["pop_norm"] +
        0.30 * df["area_norm_inv"]
    )

    # Flood Risk Index
    df["FRI"] = 0.60 * df["H"] + 0.40 * df["V"]

    # Klasifikasi
    df["risk_level"] = pd.cut(
        df["FRI"],
        bins=[-np.inf, 0.33, 0.66, np.inf],
        labels=["low", "medium", "high"]
    )

    return df

def upsert_results(df: pd.DataFrame, period_date: date):
    with engine.connect() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO flood_risk_index
                    (kelurahan_id, risk_index, risk_level, period_date, scoring_method)
                VALUES
                    (:kelurahan_id, :risk_index, :risk_level, :period_date, 'rule_based')
                ON CONFLICT (kelurahan_id, period_date, scoring_method) DO UPDATE
                SET risk_index = EXCLUDED.risk_index,
                    risk_level = EXCLUDED.risk_level,
                    created_at = NOW()
            """), {
                "kelurahan_id": row["kelurahan_id"],
                "risk_index":   round(float(row["FRI"]), 6),
                "risk_level":   str(row["risk_level"]),
                "period_date":  period_date,
            })
        conn.commit()

def run():
    print("[*] Loading features...")
    df = load_features()
    print(f"    Kelurahan loaded: {len(df)}")

    print("[*] Computing FRI...")
    df = compute_fri(df)

    print("\n[*] Risk distribution:")
    print(df["risk_level"].value_counts().to_string())

    print("\n[*] Top 10 highest risk kelurahan:")
    top10 = df.nlargest(10, "FRI")[["nama_kelurahan", "nama_kecamatan", "FRI", "risk_level"]]
    print(top10.to_string(index=False))

    period = date.today()
    print(f"\n[*] Upserting results for period: {period}")
    upsert_results(df, period)

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT risk_level, COUNT(*) as total
            FROM flood_risk_index
            WHERE period_date = :period
            GROUP BY risk_level ORDER BY risk_level
        """), {"period": period}).fetchall()
        print("\n[=] Saved to PostGIS:")
        for r in result:
            print(f"    {r[0]}: {r[1]} kelurahan")

if __name__ == "__main__":
    mkdir_cmd = "mkdir -p ~/projects/flood-pipeline/scripts/scoring"
    run()
