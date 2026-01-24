{{ config(materialized='view') }}

select
  -- Core identifiers
  cast(post_id as string) as post_id,
  lower(cast(subreddit as string)) as subreddit,

  -- Timestamps
  cast(created_ts as timestamp) as created_ts,
  cast(ingested_ts as timestamp) as ingested_ts,

  -- Text cleaning: trim + convert empty strings to NULL
  nullif(trim(cast(title as string)), '') as title,
  nullif(trim(cast(selftext as string)), '') as selftext,

  -- Metrics: ensure integer type
  cast(score as int64) as score,
  cast(num_comments as int64) as num_comments,

  -- Extract from JSON (row_json is STRING containing JSON)
  json_value(raw_json, '$.author') as author,
  concat('https://www.reddit.com', json_value(raw_json, '$.permalink')) as post_url

from {{ source('bronze_layer', 'reddit_posts_raw') }}
