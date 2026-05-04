import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

load_dotenv("/home/keima/projects/flood-pipeline/.env")

DB_URL = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
engine = create_engine(DB_URL)

CSV_PATH = "/home/keima/projects/flood-pipeline/data/raw/20260429_080257.csv"

# Mapping kabupaten ke centroid koordinat untuk geometry
KABUPATEN_COORDS = {
    "Bandung":      (-7.0583, 107.5780),
    "Kota Bandung": (-6.9175, 107.6191),
}

def load_csv() -> pd.DataFrame:
    df = pd.read_csv(CSV_PATH)
    df.columns = [c.strip() for c in df.columns]
    return df

def process(df: pd.DataFrame) -> pd.DataFrame:
    df["event_date"] = pd.to_datetime(
        df["Tanggal / Waktu Kejadian"], errors="coerce"
    ).dt.date
    df["kabupaten"]        = df["Kabupaten"].str.strip()
    df["jumlah_kejadian"]  = df["Jumlah Kejadian"].fillna(1).astype(int)
    df["menderita"]        = df["menderita_mengungsi"].fillna(0).astype(int)
    df["rumah_terendam"]   = df["Rumah Terendam"].fillna(0).astype(int)
    df["severity"] = df["menderita"].apply(
        lambda x: "high" if x > 1000 else "medium" if x > 100 else "low"
    )
    return df[["event_date", "kabupaten", "jumlah_kejadian", "menderita", "rumah_terendam", "severity"]]

def upsert_to_postgis(df: pd.DataFrame):
    # Buat tabel baru untuk flood events level kabupaten dari DIBI
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS flood_events_dibi (
                id              SERIAL PRIMARY KEY,
                event_date      DATE,
                kabupaten       VARCHAR,
                jumlah_kejadian INTEGER,
                menderita       INTEGER,
                rumah_terendam  INTEGER,
                severity        VARCHAR,
                geom            GEOMETRY(POINT, 4326),
                source          VARCHAR DEFAULT 'dibi_bnpb',
                UNIQUE(event_date, kabupaten)
            )
        """))
        conn.commit()

        inserted = 0
        for _, row in df.iterrows():
            coords = KABUPATEN_COORDS.get(row["kabupaten"])
            if not coords:
                continue
            lat, lon = coords
            conn.execute(text("""
                INSERT INTO flood_events_dibi
                    (event_date, kabupaten, jumlah_kejadian, menderita, rumah_terendam, severity, geom, source)
                VALUES
                    (:event_date, :kabupaten, :jumlah_kejadian, :menderita, :rumah_terendam, :severity,
                     ST_SetSRID(ST_MakePoint(:lon, :lat), 4326), 'dibi_bnpb')
                ON CONFLICT (event_date, kabupaten) DO UPDATE
                SET jumlah_kejadian = EXCLUDED.jumlah_kejadian,
                    menderita       = EXCLUDED.menderita,
                    rumah_terendam  = EXCLUDED.rumah_terendam,
                    severity        = EXCLUDED.severity
            """), {
                "event_date":      str(row["event_date"]),
                "kabupaten":       row["kabupaten"],
                "jumlah_kejadian": int(row["jumlah_kejadian"]),
                "menderita":       int(row["menderita"]),
                "rumah_terendam":  int(row["rumah_terendam"]),
                "severity":        row["severity"],
                "lon":             lon,
                "lat":             lat,
            })
            inserted += 1
        conn.commit()
    return inserted

def run():
    print("[*] Loading DIBI CSV...")
    df = load_csv()
    print(f"    Raw rows: {len(df)}")

    df = process(df)
    print(f"    Processed rows: {len(df)}")
    print(f"    Date range: {df['event_date'].min()} to {df['event_date'].max()}")
    print(f"    Severity dist:\n{df['severity'].value_counts().to_string()}")

    print("[*] Upserting to PostGIS...")
    inserted = upsert_to_postgis(df)
    print(f"[=] Inserted/updated: {inserted} records")

if __name__ == "__main__":
    run()
