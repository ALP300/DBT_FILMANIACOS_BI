{{ config(
    materialized='table',
    tags=['dimension', 'date']
) }}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2005-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
),

final as (
    select
        cast(date_day as date) as date_key,
        date_day,
        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        extract(month from date_day) as month,
        extract(day from date_day) as day,
        extract(dow from date_day) as day_of_week,
        extract(doy from date_day) as day_of_year,
        to_char(date_day, 'Month') as month_name,
        to_char(date_day, 'Day') as day_name,
        case when extract(dow from date_day) in (0, 6) then true else false end as is_weekend
    from date_spine
)

select * from final