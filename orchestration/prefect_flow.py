from __future__ import annotations

import os
import subprocess
from pathlib import Path

from prefect import flow, task, get_run_logger

# ==== Paths (edit only if your repo differs) ====
PROJECT_ROOT = Path(r"C:\reddit-topic-warehouse")

INGEST_SCRIPT = PROJECT_ROOT /  "src" / "ingest" / "load_reddit_bronze_batch.py"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "models" / "reddit_topic_warehouse"
# ===============================================


def run_cmd(cmd: list[str], cwd: Path) -> None:
    """Run a command, stream logs, fail loudly if it errors."""
    logger = get_run_logger()
    logger.info(f"Running: {' '.join(cmd)} (cwd={cwd})")

    completed = subprocess.run(
        cmd,
        cwd=str(cwd),
        env=os.environ.copy(),
        text=True,
        capture_output=True,
    )

    if completed.stdout:
        logger.info(completed.stdout)
    if completed.stderr:
        logger.warning(completed.stderr)

    if completed.returncode != 0:
        raise RuntimeError(f"Command failed ({completed.returncode}): {' '.join(cmd)}")


@task(retries=2, retry_delay_seconds=30)
def bronze_ingestion() -> None:
    # Ensure we use the venv python (same interpreter Prefect uses)
    python_exe = os.sys.executable
    run_cmd([python_exe, str(INGEST_SCRIPT)], cwd=PROJECT_ROOT)


@task
def dbt_run_silver() -> None:
    run_cmd(["dbt", "run", "--select", "staging", "silver"], cwd=DBT_PROJECT_DIR)


@task
def dbt_test_silver() -> None:
    run_cmd(["dbt", "test", "--select", "staging", "silver"], cwd=DBT_PROJECT_DIR)


@task
def dbt_run_gold() -> None:
    run_cmd(["dbt", "run", "--select", "gold"], cwd=DBT_PROJECT_DIR)


@task
def dbt_test_gold() -> None:
    run_cmd(["dbt", "test", "--select", "gold"], cwd=DBT_PROJECT_DIR)


@flow(name="reddit-topic-warehouse-pipeline")
def reddit_topic_warehouse_pipeline() -> None:
    """
    End-to-end pipeline:
      1) ingest -> bronze
      2) build + test silver
      3) build + test gold
    """
    bronze_ingestion()
    dbt_run_silver()
    dbt_test_silver()
    dbt_run_gold()
    dbt_test_gold()


if __name__ == "__main__":
    reddit_topic_warehouse_pipeline()

if __name__ == "__main__":
    reddit_topic_warehouse_pipeline.serve(
        name="rtw-daily",
        cron="0 9 * * *",
        tags=["reddit", "rtw", "bronze", "dbt", "bigquery", "daily"],
        description="Daily Reddit Topic Warehouse pipeline: ingest -> dbt silver -> tests -> dbt gold -> tests",
        version="1.0.0",
    )


