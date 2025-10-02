with stores as (
    select * from {{ source('dvdrental', 'store') }}
),

addresses as (
    select * from {{ source('dvdrental', 'address') }}
),

cities as (
    select * from {{ source('dvdrental', 'city') }}
),

countries as (
    select * from {{ source('dvdrental', 'country') }}
),

final as (
    select
        s.store_id,
        s.manager_staff_id,
        
        -- Address details
        a.address,
        a.district,
        a.postal_code,
        a.phone,
        
        -- Location
        ci.city,
        co.country,
        
        s.last_update
    from stores s
    left join addresses a on s.address_id = a.address_id
    left join cities ci on a.city_id = ci.city_id
    left join countries co on ci.country_id = co.country_id
)

select * from final