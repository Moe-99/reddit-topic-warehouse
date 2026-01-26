from __future__ import annotations

import os
import json
import subprocess
import urllib.request
import urllib.error
from pathlib import Path
from datetime import datetime, timezone
from google.cloud import bigquery



from prefect import flow, task, get_run_logger

# ==== Paths ====
PROJECT_ROOT = Path(r"C:\reddit-topic-warehouse")

INGEST_SCRIPT = PROJECT_ROOT / "src" / "ingest" / "load_reddit_bronze_batch.py"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "models" / "reddit_topic_warehouse"
# ===============================================


def run_cmd(cmd: list[str], cwd: Path) -> None:
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


# =========================
# Option 3: Trigger GitHub Actions
# =========================
@task(retries=2, retry_delay_seconds=30)
def trigger_github_dbt_workflow(run_id: str = "") -> None:
    """
    Triggers GitHub Actions workflow_dispatch for .github/workflows/pipeline.yml

    Required env vars (set locally on your laptop):
      - GITHUB_TOKEN
      - GITHUB_OWNER
      - GITHUB_REPO

    Optional:
      - GITHUB_WORKFLOW_FILE (default: pipeline.yml)
      - GITHUB_REF (default: main)
    """
    logger = get_run_logger()

    token = os.environ["GITHUB_TOKEN"]
    owner = os.environ["GITHUB_OWNER"]
    repo = os.environ["GITHUB_REPO"]
    workflow_file = os.getenv("GITHUB_WORKFLOW_FILE", "pipeline.yml")
    ref = os.getenv("GITHUB_REF", "main")

    url = f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/{workflow_file}/dispatches"

    payload = {"ref": ref, "inputs": {"run_id": run_id}}
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
            "User-Agent": "prefect-trigger",
        },
    )

    try:
        with urllib.request.urlopen(req) as resp:
            if resp.status not in (201, 204):
                raise RuntimeError(f"Unexpected status {resp.status}")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"GitHub dispatch failed: {e.code} {e.reason} | {body}")

    logger.info(f"✅ Triggered GitHub Actions workflow '{workflow_file}' on ref '{ref}' (run_id={run_id})")


# =========================
# Existing tasks
# =========================
@task(retries=2, retry_delay_seconds=30)
def bronze_ingestion() -> None:
    python_exe = PROJECT_ROOT / "dbt" / "models" / "reddit_topic_warehouse" / ".venv" / "Scripts" / "python.exe"
    run_cmd([str(python_exe), str(INGEST_SCRIPT)], cwd=PROJECT_ROOT)



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

@task
def check_bronze_freshness() -> None:
    client = bigquery.Client()
    query = """
    SELECT COUNT(*) as cnt
    FROM `virtual-flux-455815-k4.bronze_layer.reddit_posts_raw`
    WHERE ingested_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 26 HOUR)
    """
    result = list(client.query(query))[0]["cnt"]

    if result == 0:
        raise ValueError("No new rows ingested into bronze in the last 26 hours!")
    else:
        get_run_logger().info(f"Bronze freshness check passed: {result} rows")



@flow(name="reddit-topic-warehouse-pipeline")
def reddit_topic_warehouse_pipeline() -> None:
    """
    Option 3 end-to-end pipeline:
      1) Laptop: ingest -> bronze (Reddit access works locally)
      2) Laptop: validate bronze freshness
      3) GitHub Actions: dbt run/test -> staging/silver/gold (cloud runner)
    """
    bronze_ingestion()
    check_bronze_freshness()

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    trigger_github_dbt_workflow(run_id=run_id)



if __name__ == "__main__":
    reddit_topic_warehouse_pipeline()
