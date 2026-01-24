{{ config(materialized='view') }}
SELECT
    -- Core identifiers
    CAST(post_id as string) as post_id,
    LOWER(CAST(subreddit as string)) as subreddit,
    
    --Timestamps
    CAST(created_ts as timestamp) as created_ts,
    CAST(ingested_ts as timestamp) as ingested_ts,

    --Text cleaning: trim + convert empty strings to NULL
    NULLIF(TRIM(CAST(title as string)), '') as title,
    NULLIF(TRIM(CAST(selftext as string)), '') as selftext,

    -- Metrics: ensure integer type
    CAST(score as integer) as score,
    CAST(num_comments as integer) as num_comments,

    -- Extract from JSON (row_json is STRING containing JSON)
    json_value(raw_json, '$.author') as author,
    concat('https://www.reddit.com', json_value(raw_json, '$.permalink')) as post_url

    from {{ source('bronze_layer', 'reddit_posts_raw') }}





