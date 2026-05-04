CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS kelurahan (
  id VARCHAR PRIMARY KEY,
  nama_kelurahan VARCHAR NOT NULL,
  nama_kecamatan VARCHAR,
  luas_km2 FLOAT,
  population INTEGER,
  pop_density FLOAT,
  geom GEOMETRY(MULTIPOLYGON, 4326)
);

CREATE INDEX IF NOT EXISTS idx_kelurahan_geom ON kelurahan USING GIST(geom);

CREATE TABLE IF NOT EXISTS rainfall_daily (
    id              SERIAL PRIMARY KEY,
    kelurahan_id    VARCHAR REFERENCES kelurahan(id),
    date            DATE NOT NULL,
    precipitation   FLOAT,
    rain_sum        FLOAT,
    source          VARCHAR DEFAULT 'open_meteo',
    UNIQUE(kelurahan_id, date, source)
);
CREATE INDEX IF NOT EXISTS idx_rainfall_date ON rainfall_daily(date);
CREATE INDEX IF NOT EXISTS idx_rainfall_kelurahan ON rainfall_daily(kelurahan_id);

-- Elevation stats per kelurahan
CREATE TABLE IF NOT EXISTS elevation_stats (
    kelurahan_id    VARCHAR PRIMARY KEY REFERENCES kelurahan(id),
    mean_elevation  FLOAT,
    min_elevation   FLOAT,
    max_elevation   FLOAT,
    mean_slope      FLOAT,
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Historical flood events
CREATE TABLE IF NOT EXISTS flood_events (
    id              SERIAL PRIMARY KEY,
    kelurahan_id    VARCHAR REFERENCES kelurahan(id),
    event_date      DATE,
    source          VARCHAR,
    severity        VARCHAR,
    geom            GEOMETRY(POINT, 4326)
);
CREATE INDEX IF NOT EXISTS idx_flood_events_geom ON flood_events USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_flood_events_date ON flood_events(event_date);

-- Rule-based risk output
CREATE TABLE IF NOT EXISTS flood_risk_index (
    id              SERIAL PRIMARY KEY,
    kelurahan_id    VARCHAR REFERENCES kelurahan(id),
    risk_index      FLOAT,
    risk_level      VARCHAR CHECK(risk_level IN ('low', 'medium', 'high')),
    period_date     DATE,
    scoring_method  VARCHAR DEFAULT 'rule_based',
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(kelurahan_id, period_date, scoring_method)
);

-- ML output
CREATE TABLE IF NOT EXISTS flood_risk_ml (
    id              SERIAL PRIMARY KEY,
    kelurahan_id    VARCHAR REFERENCES kelurahan(id),
    risk_level      VARCHAR CHECK(risk_level IN ('low', 'medium', 'high')),
    probability     FLOAT CHECK(probability BETWEEN 0 AND 1),
    model_version   VARCHAR,
    period_date     DATE,
    predicted_at    TIMESTAMP DEFAULT NOW(),
    UNIQUE(kelurahan_id, period_date, model_version)
);