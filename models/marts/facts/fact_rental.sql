{{ config(
    materialized='table',
    tags=['fact']
) }}

with rentals as (
    select * from {{ ref('stg_rental') }}
),

inventory as (
    select * from {{ ref('stg_inventory') }}
),

payments as (
    select 
        rental_id,
        sum(amount) as total_payment_amount
    from {{ ref('stg_payment') }}
    group by rental_id
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['r.rental_id']) }} as rental_key,
        
        -- Natural keys
        r.rental_id,
        
        -- Foreign keys (dimensions)
        r.customer_id,
        i.film_id,
        r.staff_id,
        i.store_id,
        cast(r.rental_date as date) as rental_date_key,
        cast(r.return_date as date) as return_date_key,
        
        -- Degenerate dimensions
        r.inventory_id,
        
        -- Dates
        r.rental_date,
        r.return_date,
        
        -- Metrics/Facts
        coalesce(p.total_payment_amount, 0) as payment_amount,
        
        -- Calculated metrics
        case 
            when r.return_date is not null 
            then extract(day from (r.return_date - r.rental_date))
            else null 
        end as rental_duration_days,
        
        case 
            when r.return_date is null then 1 
            else 0 
        end as is_currently_rented,
        
        r.last_update
    from rentals r
    left join inventory i on r.inventory_id = i.inventory_id
    left join payments p on r.rental_id = p.rental_id
)

select * from final