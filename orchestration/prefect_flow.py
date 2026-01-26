from __future__ import annotations
import os
import 
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
def trigger_github_dbt_workflow(run_id: str) -> dict:
    """
    Trigger GitHub Actions workflow_dispatch and return info needed for polling.
    """
    logger = get_run_logger()

    token = os.environ["GITHUB_TOKEN"]
    owner = os.environ["GITHUB_OWNER"]
    repo = os.environ["GITHUB_REPO"]
    workflow_file = os.getenv("GITHUB_WORKFLOW_FILE", "pipeline.yml")
    ref = os.getenv("GITHUB_REF", "main")

    dispatch_url = f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/{workflow_file}/dispatches"
    payload = {"ref": ref, "inputs": {"run_id": run_id}}
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        dispatch_url,
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

    logger.info(f"✅ Dispatched workflow '{workflow_file}' on ref '{ref}' (run_id={run_id})")

    return {
        "owner": owner,
        "repo": repo,
        "workflow_file": workflow_file,
        "ref": ref,
        "run_id": run_id,
    }


@task(retries=0)
def wait_for_github_workflow(info: dict, timeout_seconds: int = 60 * 30, poll_seconds: int = 15) -> None:
    """
    Wait for the GitHub Actions run with display_title containing run_id to finish.
    Fails Prefect if GitHub run fails.
    """
    logger = get_run_logger()

    token = os.environ["GITHUB_TOKEN"]
    owner = info["owner"]
    repo = info["repo"]
    workflow_file = info["workflow_file"]
    run_id = info["run_id"]

    list_runs_url = f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/{workflow_file}/runs?per_page=20"

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "prefect-poller",
    }

    start = time.time()
    found_run = None

    # Step A: Find the run that matches our run_id in the run title
    while time.time() - start < timeout_seconds:
        req = urllib.request.Request(list_runs_url, headers=headers, method="GET")
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        runs = data.get("workflow_runs", [])
        for r in runs:
            display_title = (r.get("display_title") or "")
            if run_id in display_title:
                found_run = r
                break

        if found_run:
            break

        logger.info(f"Waiting for GitHub run to appear... (run_id={run_id})")
        time.sleep(poll_seconds)

    if not found_run:
        raise RuntimeError(f"Timed out waiting for GitHub run to appear (run_id={run_id})")

    run_api_url = found_run["url"]  # API URL for this specific run
    html_url = found_run.get("html_url", "")
    logger.info(f"✅ Found GitHub run: {html_url}")

    # Step B: Poll until completed
    while time.time() - start < timeout_seconds:
        req = urllib.request.Request(run_api_url, headers=headers, method="GET")
        with urllib.request.urlopen(req) as resp:
            run = json.loads(resp.read().decode("utf-8"))

        status = run.get("status")          # queued / in_progress / completed
        conclusion = run.get("conclusion")  # success / failure / cancelled / etc.

        logger.info(f"GitHub run status={status}, conclusion={conclusion}")

        if status == "completed":
            if conclusion == "success":
                logger.info(f"✅ GitHub workflow succeeded: {html_url}")
                return
            raise RuntimeError(f"❌ GitHub workflow failed: conclusion={conclusion} | {html_url}")

        time.sleep(poll_seconds)

    raise RuntimeError(f"Timed out waiting for GitHub run to complete (run_id={run_id})")



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
    info = trigger_github_dbt_workflow(run_id=run_id)
    wait_for_github_workflow(info, timeout_seconds=60 * 30, poll_seconds=15)



if __name__ == "__main__":
    reddit_topic_warehouse_pipeline()
