{{ config(materialized='table') }}

select
  subreddit,
  min(date(created_ts)) as first_seen_date,
  max(date(created_ts)) as last_seen_date,
  count(*) as posts_total,
  avg(score) as avg_score_all_time,
  avg(num_comments) as avg_comments_all_time
from {{ ref('reddit_posts_clean') }}
group by subreddit
order by posts_total desc
