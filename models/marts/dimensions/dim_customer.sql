-- Este proceso ETL unifica la información de clientes tomando datos de la tabla staging (stg_customer) 
-- y los enriquece con detalles de dirección, ciudad y país desde las tablas fuente (dvdrental). 
-- El resultado es una vista consolidada de clientes con datos personales, de contacto y localización.


with customers as (
    select * from {{ ref('stg_customer') }}
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
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.active,
        c.create_date,
        c.store_id,
        
        -- Address details
        a.address,
        a.address2,
        a.district,
        a.postal_code,
        a.phone,
        
        -- City and country
        ci.city,
        co.country,
        
        c.last_update
    from customers c
    left join addresses a on c.address_id = a.address_id
    left join cities ci on a.city_id = ci.city_id
    left join countries co on ci.country_id = co.country_id
)

select * from final