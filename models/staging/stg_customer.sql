with source as (
    select * from {{ source('dvdrental', 'customer') }}
),

renamed as (
    select
        customer_id,
        store_id,
        first_name,
        last_name,
        email,
        address_id,
        active,
        create_date,
        last_update
    from source
)

select * from renamed