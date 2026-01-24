{{ config(materialized='table') }}

-- Build a date dimension only for the date range we actually have in the data.
with bounds as (
  select
    min(date(created_ts)) as min_date,
    max(date(created_ts)) as max_date
  from {{ ref('reddit_posts_clean') }}
),

date_spine as (
  select d as date_day
  from bounds,
  unnest(generate_date_array(min_date, max_date)) as d
)

select
  date_day,
  extract(year from date_day) as year,
  extract(month from date_day) as month,
  extract(day from date_day) as day,
  extract(dayofweek from date_day) as day_of_week,         -- 1=Sunday ... 7=Saturday (BigQuery)
  format_date('%A', date_day) as day_name,
  extract(isoweek from date_day) as iso_week,
  extract(isoyear from date_day) as iso_year,
  format_date('%Y-%m', date_day) as year_month,
  case when extract(dayofweek from date_day) in (1, 7) then true else false end as is_weekend
from date_spine
order by date_day
