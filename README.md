# Bandung Flood Risk Pipeline 🌊

A reproducible data engineering pipeline for mapping flood risk across 151 kelurahans (sub-districts) in Bandung, Indonesia. Built on an open-source stack, automatically refreshed daily.

**Live Dashboard:** [data.godwimp.me](https://data.godwimp.me)  
**Pipeline UI:** [prefect.godwimp.me](https://prefect.godwimp.me)

---

## Overview

This pipeline combines elevation data, historical rainfall, population density, river proximity, and flood event records to compute a **Flood Risk Index (FRI)** per kelurahan using an AHP-GIS methodology referenced from Indonesian urban flood studies and BNPB Regulation No. 2/2012.

The output is a scored, classified risk layer (`low` / `medium` / `high`) that refreshes automatically and is visualized in a Metabase dashboard.

---

## Architecture

```
Data Sources
├── Open-Meteo Historical API  → Rainfall (2020–present)
├── OpenTopoData / SRTM 30m   → Elevation & slope
├── OpenStreetMap Overpass     → River network
├── Open Data Bandung          → Population per kelurahan
├── PetaBencana.id             → Near-realtime flood events
└── DIBI BNPB                  → Historical flood events (2021–2026)

Ingestion (Python) → MinIO (raw storage)
Processing (Python + GeoPandas) → PostgreSQL + PostGIS
Risk Scoring (Python) → flood_risk_index table
dbt → analytics schema (staging → intermediate → mart)
Prefect → Scheduling & orchestration
Metabase → Dashboard
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.12 |
| Spatial processing | GeoPandas, Rasterio, Shapely, GDAL |
| Orchestration | Prefect 3 |
| Raw storage | MinIO (S3-compatible) |
| Processed storage | PostgreSQL 16 + PostGIS 3.4 |
| SQL modeling | dbt 1.11 |
| Visualization | Metabase |
| Infrastructure | DigitalOcean Droplet, Docker Compose |
| Reverse proxy | Nginx + Certbot (Let's Encrypt) + Cloudflare |

---

## Flood Risk Index Formula

**Hazard Score:**

```
H = 0.25·ê_inv + 0.20·ŝ_inv + 0.20·r̂ + 0.20·f̂ + 0.15·d̂_inv
```

**Vulnerability Score:**

```
V = 0.70·p̂ + 0.30·â_inv
```

**Flood Risk Index:**

```
FRI = 0.60 · H + 0.40 · V     FRI ∈ [0, 1]
```

**Classification:**
- `low` : FRI < 0.33
- `medium` : 0.33 ≤ FRI < 0.66
- `high` : FRI ≥ 0.66

Full methodology documented in [`docs/formula.md`](docs/formula.md).

---

## Project Structure

```
bandung-flood-risk-pipeline/
├── scripts/
│   ├── ingestion/
│   │   ├── ingest_rainfall.py
│   │   ├── ingest_population.py
│   │   ├── ingest_boundary.py
│   │   ├── ingest_dem.py
│   │   ├── ingest_rivers.py
│   │   └── ingest_flood_events.py
│   ├── processing/
│   │   ├── process_rainfall.py
│   │   ├── process_population.py
│   │   ├── process_boundary.py
│   │   ├── process_dem.py
│   │   ├── process_rivers.py
│   │   ├── process_flood_events.py
│   │   └── process_dibi_floods.py
│   ├── scoring/
│   │   └── risk_scoring.py
│   └── db/
│       └── schema.sql
├── dbt/
│   └── flood_risk/
│       ├── models/
│       │   ├── staging/
│       │   ├── intermediate/
│       │   └── mart/
│       └── dbt_project.yml
├── dags/
│   └── flows.py
├── docs/
│   ├── de-pipeline-steps.md
│   └── project-overview.md
├── docker/
│   └── docker-compose.yml
├── .env.example
├── .gitignore
├── prefect.yaml
└── README.md
```

---

## Data Sources

| Source | URL | Access | Update Frequency |
|---|---|---|---|
| Open-Meteo | historical-forecast-api.open-meteo.com | Free, no key | Daily |
| OpenTopoData SRTM | api.opentopodata.org | Free, no key | Static |
| OSM Overpass | overpass-api.de | Free, no key | Weekly |
| Open Data Bandung | opendata.bandung.go.id | Free, no key | Yearly |
| PetaBencana.id | data.petabencana.id | Free, no key | Near-realtime |
| DIBI BNPB | dibi.bnpb.go.id | Manual export | Per request |

---

## Setup

### Prerequisites
- Docker & Docker Compose
- Python 3.12+
- Git

### 1. Clone & configure

```bash
git clone https://github.com/godwimp/bandung-flood-risk-pipeline.git
cd bandung-flood-risk-pipeline
cp .env.example .env
# Edit .env with your credentials
```

### 2. Start services

```bash
docker compose -f docker/docker-compose.yml up -d
```

### 3. Install Python dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install prefect geopandas rasterio shapely psycopg2-binary \
  sqlalchemy geoalchemy2 pandas numpy requests python-dotenv minio dbt-postgres
```

### 4. Initialize database

```bash
docker exec -i postgis psql -U your_user -d flooddb < scripts/db/schema.sql
```

### 5. Run full pipeline

```bash
# Manual trigger via Prefect
prefect deploy --all
prefect worker start --pool default-agentpool
prefect deployment run 'full-pipeline/full-pipeline'
```

---

## Pipeline Schedules

| Flow | Schedule | Description |
|---|---|---|
| `daily-rainfall` | Daily 01:00 WIB | Ingest + process rainfall → scoring → dbt |
| `daily-flood-events` | Daily 02:00 WIB | Ingest + process flood events → scoring → dbt |
| `monthly-static` | 1st of month 03:00 WIB | DEM, rivers, population, boundary |
| `full-pipeline` | Manual | Run everything from scratch |

---

## Results (May 2026)

| Risk Level | Kelurahan | Percentage |
|---|---|---|
| High | 1 | 0.66% |
| Medium | 135 | 89.40% |
| Low | 15 | 9.93% |

Highest risk kelurahan: **Babakan Tarogong**, Bojongloa Kaler (FRI: 0.704)

---

## Documentation

- [DE Pipeline Steps](docs/de-pipeline-steps.md) — Step-by-step data engineering documentation
- [Project Overview](docs/project-overview.md) — Full project overview & task division

---

## Roadmap

- [x] Infrastructure setup
- [x] Data ingestion (6 sources)
- [x] Spatial processing
- [x] Rule-based risk scoring (AHP-GIS)
- [x] dbt modeling (8 models, 16 tests)
- [x] Prefect orchestration (4 flows)
- [x] Metabase dashboard
- [ ] ML flood probability model (in progress)
- [ ] ML model integration to pipeline

---

## Contributing

This project is part of a portfolio. ML/AI integration (flood probability model) is in progress by a collaborating ML Engineer.

---

## License

MIT License

