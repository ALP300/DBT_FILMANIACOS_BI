with staff as (
    select * from {{ source('dvdrental', 'staff') }}
),

addresses as (
    select * from {{ source('dvdrental', 'address') }}
),

stores as (
    select * from {{ source('dvdrental', 'store') }}
),

final as (
    select
        s.staff_id,
        s.first_name,
        s.last_name,
        s.email,
        s.username,
        s.active,
        
        -- Address
        a.address,
        a.city_id,
        
        -- Store
        s.store_id,
        
        s.last_update
    from staff s
    left join addresses a on s.address_id = a.address_id
)

select * from final