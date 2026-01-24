-- models/silver/reddit_posts_clean.sql

{{ config(materialized='table') }}

with ranked as (
  select
    *,
    row_number() over (
      partition by post_id
      order by ingested_ts desc
    ) as rn
  from {{ ref('stg_reddit_posts') }}
)

select
  post_id,
  subreddit,
  created_ts,
  ingested_ts,
  title,
  selftext,
  score,
  num_comments,
  author,
  post_url
from ranked
where rn = 1
