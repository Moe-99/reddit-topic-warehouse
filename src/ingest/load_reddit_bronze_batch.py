import requests
import json
import time
from datetime import datetime, timezone
from google.cloud import bigquery

PROJECT_ID = "virtual-flux-455815-k4"
DATASET_ID = "bronze_layer"
TABLE_ID = "reddit_posts_raw"

HEADERS = {"User-Agent": "reddit-topic-warehouse/0.1 (learning project)"}

def fetch_new_posts(subreddit: str, limit: int = 5):
    url = f"https://www.reddit.com/r/{subreddit}/new.json"
    headers = {
        # Reddit really cares about this. Make it specific.
        "User-Agent": "reddit-topic-warehouse/1.0 (by u/your_username; contact: your_email)"
    }
    params = {"limit": limit, "raw_json": 1}

    # small retry for transient blocks/rate limits
    for attempt in range(3):
        resp = requests.get(url, headers=headers, params=params, timeout=20)

        # Sometimes 429/403 clears on retry; 403 may not, but try.
        if resp.status_code in (403, 429):
            time.sleep(2 * (attempt + 1))
            continue

        resp.raise_for_status()
        data = resp.json()
        return data["data"]["children"]

    # If we got here, it's consistently blocked
    resp.raise_for_status()

def to_bronze_record(post: dict, subreddit: str) -> dict:
    created_utc = post.get("created_utc")
    created_ts = None
    if created_utc is not None:
        created_ts = datetime.fromtimestamp(created_utc, tz=timezone.utc).isoformat()

    ingested_ts = datetime.now(timezone.utc).isoformat()

    return {
        "post_id": post.get("id"),
        "subreddit": subreddit,
        "created_ts": created_ts,
        "ingested_ts": ingested_ts,
        "title": post.get("title"),
        "selftext": post.get("selftext"),
        "score": post.get("score"),
        "num_comments": post.get("num_comments"),
        "raw_json": json.dumps(post, ensure_ascii=False),
    }

def write_ndjson(rows: list[dict], filepath: str):
    with open(filepath, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

def load_ndjson_to_bigquery(client: bigquery.Client, filepath: str):
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    with open(filepath, "rb") as f:
        job = client.load_table_from_file(f, table_ref, job_config=job_config)

    job.result()  # Waits for the job to complete
    print(f"Loaded {job.output_rows} rows into {table_ref}")

def main():
    subreddits = ["worldnews", "news", "technology", "sports", "gaming", "science"]
    limit_per_subreddit = 5

    bronze_rows = []
    for sub in subreddits:
        print(f"Fetching {limit_per_subreddit} posts from r/{sub}...")
        posts = fetch_new_posts(sub, limit=limit_per_subreddit)
        for post in posts:
            bronze_rows.append(to_bronze_record(post, subreddit=sub))
        time.sleep(1)

    print(f"Built {len(bronze_rows)} Bronze rows")

    # Write to local file
    filepath = "bronze_reddit_posts.ndjson"
    write_ndjson(bronze_rows, filepath)
    print(f"Wrote NDJSON file: {filepath}")

    # Load into BigQuery using a load job (free-tier friendly)
    client = bigquery.Client(project=PROJECT_ID)
    load_ndjson_to_bigquery(client, filepath)

if __name__ == "__main__":
    main()
