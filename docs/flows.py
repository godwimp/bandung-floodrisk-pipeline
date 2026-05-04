import sys
import subprocess
sys.path.insert(0, "/home/keima/projects/flood-pipeline")

from prefect import flow, task, get_run_logger

from scripts.ingestion.ingest_rainfall       import run as ingest_rainfall
from scripts.ingestion.ingest_flood_events   import run as ingest_flood_events
from scripts.ingestion.ingest_population     import run as ingest_population
from scripts.ingestion.ingest_boundary       import run as ingest_boundary
from scripts.ingestion.ingest_dem            import run as ingest_dem
from scripts.ingestion.ingest_rivers         import run as ingest_rivers
from scripts.processing.process_rainfall     import run as process_rainfall
from scripts.processing.process_population   import run as process_population
from scripts.processing.process_boundary     import run as process_boundary
from scripts.processing.process_dem          import run as process_dem
from scripts.processing.process_rivers       import run as process_rivers
from scripts.processing.process_flood_events import run as process_flood_events
from scripts.scoring.risk_scoring            import run as run_scoring

DBT_DIR = "/home/keima/projects/flood-pipeline/dbt/flood_risk"
VENV_DBT = "/home/keima/projects/flood-pipeline/.venv/bin/dbt"

# ── Tasks ──────────────────────────────────────────────

@task(name="ingest-rainfall", retries=2, retry_delay_seconds=30)
def task_ingest_rainfall():
    get_run_logger().info("Ingesting rainfall...")
    ingest_rainfall()

@task(name="ingest-flood-events", retries=2, retry_delay_seconds=30)
def task_ingest_flood_events():
    get_run_logger().info("Ingesting flood events...")
    ingest_flood_events()

@task(name="ingest-population", retries=1, retry_delay_seconds=60)
def task_ingest_population():
    get_run_logger().info("Ingesting population...")
    ingest_population()

@task(name="ingest-boundary", retries=1, retry_delay_seconds=60)
def task_ingest_boundary():
    get_run_logger().info("Ingesting boundary...")
    ingest_boundary()

@task(name="ingest-dem", retries=1, retry_delay_seconds=60)
def task_ingest_dem():
    get_run_logger().info("Ingesting DEM...")
    ingest_dem()

@task(name="ingest-rivers", retries=1, retry_delay_seconds=60)
def task_ingest_rivers():
    get_run_logger().info("Ingesting rivers...")
    ingest_rivers()

@task(name="process-rainfall", retries=2, retry_delay_seconds=30)
def task_process_rainfall():
    get_run_logger().info("Processing rainfall...")
    process_rainfall()

@task(name="process-flood-events", retries=2, retry_delay_seconds=30)
def task_process_flood_events():
    get_run_logger().info("Processing flood events...")
    process_flood_events()

@task(name="process-population", retries=1, retry_delay_seconds=60)
def task_process_population():
    get_run_logger().info("Processing population...")
    process_population()

@task(name="process-boundary", retries=1, retry_delay_seconds=60)
def task_process_boundary():
    get_run_logger().info("Processing boundary...")
    process_boundary()

@task(name="process-dem", retries=1, retry_delay_seconds=60)
def task_process_dem():
    get_run_logger().info("Processing DEM...")
    process_dem()

@task(name="process-rivers", retries=1, retry_delay_seconds=60)
def task_process_rivers():
    get_run_logger().info("Processing rivers...")
    process_rivers()

@task(name="risk-scoring", retries=2, retry_delay_seconds=30)
def task_risk_scoring():
    get_run_logger().info("Running risk scoring...")
    run_scoring()

@task(name="dbt-run", retries=1, retry_delay_seconds=30)
def task_dbt_run():
    logger = get_run_logger()
    logger.info("Running dbt models...")
    result = subprocess.run(
        [VENV_DBT, "run"],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt run failed:\n{result.stderr}")
    logger.info("dbt run completed successfully")

@task(name="dbt-test", retries=1, retry_delay_seconds=30)
def task_dbt_test():
    logger = get_run_logger()
    logger.info("Running dbt tests...")
    result = subprocess.run(
        [VENV_DBT, "test"],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt test failed:\n{result.stderr}")
    logger.info("dbt tests passed")

# ── Flows ──────────────────────────────────────────────

@flow(name="daily-rainfall-pipeline")
def flow_daily_rainfall():
    ingest  = task_ingest_rainfall.submit()
    process = task_process_rainfall.submit(wait_for=[ingest])
    scoring = task_risk_scoring.submit(wait_for=[process])
    dbt     = task_dbt_run.submit(wait_for=[scoring])
    task_dbt_test.submit(wait_for=[dbt])

@flow(name="daily-flood-events-pipeline")
def flow_daily_flood_events():
    ingest  = task_ingest_flood_events.submit()
    process = task_process_flood_events.submit(wait_for=[ingest])
    scoring = task_risk_scoring.submit(wait_for=[process])
    dbt     = task_dbt_run.submit(wait_for=[scoring])
    task_dbt_test.submit(wait_for=[dbt])

@flow(name="monthly-static-pipeline")
def flow_monthly_static():
    pop      = task_ingest_population.submit()
    boundary = task_ingest_boundary.submit()
    dem      = task_ingest_dem.submit()
    rivers   = task_ingest_rivers.submit()

    proc_pop      = task_process_population.submit(wait_for=[pop])
    proc_boundary = task_process_boundary.submit(wait_for=[boundary])
    proc_dem      = task_process_dem.submit(wait_for=[dem])
    proc_rivers   = task_process_rivers.submit(wait_for=[rivers])

    scoring = task_risk_scoring.submit(wait_for=[proc_pop, proc_boundary, proc_dem, proc_rivers])
    dbt     = task_dbt_run.submit(wait_for=[scoring])
    task_dbt_test.submit(wait_for=[dbt])

@flow(name="full-pipeline")
def flow_full_pipeline():
    pop      = task_ingest_population.submit()
    boundary = task_ingest_boundary.submit()
    dem      = task_ingest_dem.submit()
    rivers   = task_ingest_rivers.submit()

    proc_pop      = task_process_population.submit(wait_for=[pop])
    proc_boundary = task_process_boundary.submit(wait_for=[boundary])
    proc_dem      = task_process_dem.submit(wait_for=[dem])
    proc_rivers   = task_process_rivers.submit(wait_for=[rivers])

    rain_ingest  = task_ingest_rainfall.submit()
    flood_ingest = task_ingest_flood_events.submit()

    rain_proc  = task_process_rainfall.submit(wait_for=[rain_ingest])
    flood_proc = task_process_flood_events.submit(wait_for=[flood_ingest])

    scoring = task_risk_scoring.submit(wait_for=[
        proc_pop, proc_boundary, proc_dem, proc_rivers,
        rain_proc, flood_proc
    ])
    dbt = task_dbt_run.submit(wait_for=[scoring])
    task_dbt_test.submit(wait_for=[dbt])

if __name__ == "__main__":
    flow_full_pipeline()
