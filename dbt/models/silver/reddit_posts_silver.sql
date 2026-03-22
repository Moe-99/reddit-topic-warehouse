CREATE OR REPLACE TABLE `virtual-flux-455815-k4.silver_layer.reddit_posts_silver` AS
WITH bronze AS (
  SELECT
    run_id,
    ingested_at,
    source,
    subreddit,
    params,
    raw_response
  FROM `virtual-flux-455815-k4.bronze_layer.reddit_posts_raw`
),
posts AS (
  SELECT
    run_id,
    ingested_at,
    source,
    subreddit,
    params,
    child
  FROM bronze,
  UNNEST(JSON_QUERY_ARRAY(raw_response, '$.data.children')) AS child
)
SELECT
  run_id,
  ingested_at,
  source,
  subreddit,

  JSON_VALUE(child, '$.data.id') AS post_id,
  JSON_VALUE(child, '$.data.title') AS title,
  JSON_VALUE(child, '$.data.selftext') AS selftext,
  CAST(JSON_VALUE(child, '$.data.score') AS INT64) AS score,
  CAST(JSON_VALUE(child, '$.data.num_comments') AS INT64) AS num_comments,
  JSON_VALUE(child, '$.data.author') AS author,
  JSON_VALUE(child, '$.data.permalink') AS permalink,
  JSON_VALUE(child, '$.data.url') AS url,
  CAST(JSON_VALUE(child, '$.data.is_self') AS BOOL) AS is_self,

  TIMESTAMP_SECONDS(CAST(JSON_VALUE(child, '$.data.created_utc') AS INT64)) AS created_at_utc

FROM posts;
