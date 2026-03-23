{{config(materialized = 'table')}}

WITH ranked AS (
    SELECT
        *,
        row_number() OVER(partition by post_id order by ingested_at DESC) as rn
        FROM {{ref('stg_reddit_posts')}}
)

SELECT
  run_id,
  post_id,
  subreddit,
  created_ts,
  ingested_at,
  title,
  selftext,
  score,
  num_comments,
  author,
  post_url
from ranked
where rn = 1
