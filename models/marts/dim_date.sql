{{
  config(
    materialized = 'table',
    on_schema_change = 'fail'
  )
}}

with

date_spine as (

    {{ dbt_utils.date_spine(
        datepart   = "day",
        start_date = "to_date('2015-01-01')",
         end_date   = "dateadd(year, 20, current_date)"

    ) }}
),

final as (

    select

        date_day as date,

        --basic calendar breakdown
        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        extract(month from date_day) as month,
        extract(day from date_day) as day_of_month,
        dayofweek(date_day) as day_of_week,
        to_char(date_day, 'DY') day_of_week_short,
        to_char(date_day, 'DAY') as day_of_week_name,
        to_char(date_day, 'MON') as month_short_name,
        to_char(date_day, 'MONTH') as month_name,

        -- week / year variants
        to_char(date_day, 'YYYY-MM') as year_month,
        to_char(date_day, 'YYYY-"Q"Q') as year_quarter,
         weekofyear(date_day) as week_of_year,

        -- flags
        case when day_of_week in (1, 7) then 1 else 0 end as is_weekend,
        case when date_day = current_date then 1 else 0 end as is_today,
        case when date_day < current_date then 1 else 0 end as is_past,
        case when date_day > current_date then 1 else 0 end as is_future

    from date_spine      

)

select * from final

