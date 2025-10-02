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

