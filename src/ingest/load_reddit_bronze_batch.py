import requests
import json
import time
import uuid
from datetime import datetime, timezone
from google.cloud import bigquery
import os 
from dotenv import load_dotenv


PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID")
TABLE_ID = "reddit_posts_raw"


def fetch_raw_posts(subreddit: str, params: dict):
    reddit_url = f"https://www.reddit.com/r/{subreddit}/new.json"
    headers = {"User-Agent": "reddit-topic-warehouse/1.0 (by u/Mohamed)"}

    for attempt in range(3):
        response = requests.get(reddit_url, headers=headers, params=params, timeout=20)
        if response.status_code in (429, 403):
            time.sleep(2 ** attempt)
            continue
        response.raise_for_status()
        return response.json()

    raise Exception(f"Reddit API failed for subreddit {subreddit} after 3 attempts")


def to_bronze(payload: dict, subreddit: str, params: dict):
    ingested_at = datetime.now(timezone.utc).isoformat()

    return {
        "run_id": str(uuid.uuid4()),
        "ingested_at": ingested_at,
        "source": "reddit",
        "subreddit": subreddit,
        "params": params,
        "raw_response": payload
    }


def write_ndjson(rows: list[dict], filepath: str):
    with open(filepath, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def load_ndjson_to_bigquery(client: bigquery.Client, filepath: str):
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    with open(filepath, "rb") as f:
        job = client.load_table_from_file(f, table_ref, job_config=job_config)

    job.result()
    print(f"Loaded {job.output_rows} rows into {table_ref}")


def main():
    subreddits = ["worldnews", "news", "technology", "sports", "gaming", "science"]
    limit_per_subreddit = 5
    params = {"limit": limit_per_subreddit, "raw_json": 1}
    filepath = "bronze_reddit_posts.ndjson"

    rows = []

    for subreddit in subreddits:
        print(f"Fetching: {subreddit}")
        payload = fetch_raw_posts(subreddit, params)
        record = to_bronze(payload, subreddit, params)
        rows.append(record)

    print(f"Total bronze rows: {len(rows)}")

    write_ndjson(rows, filepath)
    print(f"Wrote NDJSON file: {filepath}")

    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()
    print(f"NDJSON lines written: {len(lines)}")
    print("First line preview:")
    print(lines[0][:500])

    client = bigquery.Client(project=PROJECT_ID)
    load_ndjson_to_bigquery(client, filepath)

if __name__ == "__main__":
    main()
