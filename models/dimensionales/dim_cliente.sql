-- models/dim/dim_cliente.sql
{{ config(
    materialized='table',
    post_hook=[
        "ALTER TABLE {{ this }} DROP CONSTRAINT IF EXISTS pk_dim_cliente;",
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='pk_dim_cliente') THEN ALTER TABLE {{ this }} ADD CONSTRAINT pk_dim_cliente PRIMARY KEY (id_cliente); END IF; END $$;",
        "CREATE INDEX IF NOT EXISTS idx_dim_cliente_nombre ON {{ this }} (nombre);"
    ]
) }}
SELECT
    customer.customer_id AS id_cliente,
    first_name AS nombre,
    last_name AS apellido,
    email,
    create_date AS fecha_registro,
    CASE 
        WHEN activebool = true THEN 'activo'
        WHEN activebool = false THEN 'inactivo'
        ELSE 'en riesgo'
    END AS estado_cliente,
    CASE 
        WHEN rental_count > 10 THEN 'frecuente'
        WHEN rental_count BETWEEN 5 AND 10 THEN 'moderado'
        ELSE 'ocasional'
    END AS segmento_cliente
FROM
    customer
JOIN (
    SELECT customer_id, COUNT(rental_id) AS rental_count
    FROM rental
    GROUP BY customer_id
 ) r ON r.customer_id = customer.customer_id
