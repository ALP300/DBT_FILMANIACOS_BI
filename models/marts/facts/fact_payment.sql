{{ config(
    materialized='table',
    tags=['fact']
) }}
with payments as (
    select * from {{ ref('stg_payment') }}
),
rentals as (
    select * from {{ ref('stg_rental') }}
),
inventory as (
    select * from {{ ref('stg_inventory') }}
),
final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['p.payment_id']) }} as payment_key,
        
        -- Natural keys
        p.payment_id,
        
        -- Foreign keys (dimensions)
        p.customer_id,
        p.staff_id,
        i.store_id,
        i.film_id,
        cast(p.payment_date as date) as payment_date_key,
        
        -- Degenerate dimensions
        p.rental_id,
        
        -- Dates
        p.payment_date,
        
        -- Metrics/Facts
        p.amount
    from payments p
    left join rentals r on p.rental_id = r.rental_id
    left join inventory i on r.inventory_id = i.inventory_id
)
select * from final