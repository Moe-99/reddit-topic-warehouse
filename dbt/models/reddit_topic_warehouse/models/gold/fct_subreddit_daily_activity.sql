{{ config(materialized='table') }}

with daily as (
  select
    date(created_ts) as date_day,
    subreddit,

    count(*) as posts_count,
    sum(score) as total_score,
    sum(num_comments) as total_comments,

    avg(score) as avg_score,
    avg(num_comments) as avg_comments
  from {{ ref('reddit_posts_clean') }}
  group by 1, 2
),

with_trends as (
  select
    *,
    (total_score + total_comments) as engagement_total,
    (avg_score + avg_comments) as engagement_avg,

    -- day-over-day change in posts
    posts_count - lag(posts_count) over (
      partition by subreddit
      order by date_day
    ) as posts_dod_change,

    -- 7-day rolling average (including today)
    avg(posts_count) over (
      partition by subreddit
      order by date_day
      rows between 6 preceding and current row
    ) as posts_7d_avg,

    -- today minus rolling average (signal above/below normal)
    posts_count - avg(posts_count) over (
      partition by subreddit
      order by date_day
      rows between 6 preceding and current row
    ) as posts_vs_7d_avg
  from daily
)

select *
from with_trends
order by date_day desc, posts_count desc
