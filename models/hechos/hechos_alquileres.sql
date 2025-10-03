-- models/fact/hechos_alquileres.sql
{{ config(
    materialized='table',
    post_hook=[
        "ALTER TABLE {{ this }} DROP CONSTRAINT IF EXISTS pk_hechos_alquileres;",
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='pk_hechos_alquileres') THEN ALTER TABLE {{ this }} ADD CONSTRAINT pk_hechos_alquileres PRIMARY KEY (id_hecho); END IF; END $$;",
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_hechos_alquileres_cliente') THEN ALTER TABLE {{ this }} ADD CONSTRAINT fk_hechos_alquileres_cliente FOREIGN KEY (id_cliente) REFERENCES {{ ref('dim_cliente') }} (id_cliente); END IF; END $$;",
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='fk_hechos_alquileres_pelicula') THEN ALTER TABLE {{ this }} ADD CONSTRAINT fk_hechos_alquileres_pelicula FOREIGN KEY (id_pelicula) REFERENCES {{ ref('dim_pelicula') }} (id_pelicula); END IF; END $$;"
    ]
) }}
WITH
    alquileres AS (
        SELECT
            a.rental_id AS id_hecho,
            a.customer_id AS id_cliente,
            f.film_id AS id_pelicula,
            f.format_id AS id_formato,
            a.promotion_id AS id_promocion,
            dt.id_tiempo AS id_tiempo,
            (
                SELECT COALESCE(SUM(pay.amount), 0)
                FROM payment pay
                WHERE pay.rental_id = a.rental_id
            ) AS monto_total,
            COUNT(a.rental_id) OVER (PARTITION BY a.customer_id) AS frecuencia_alquiler,
            a.rental_date,
            a.return_date,
            a.last_update
        FROM
            rental a
            LEFT JOIN inventory i ON a.inventory_id = i.inventory_id
            LEFT JOIN film f ON i.film_id = f.film_id
            LEFT JOIN promotion p ON a.promotion_id = p.promotion_id
            LEFT JOIN {{ ref('dim_tiempo') }} dt ON cast(a.rental_date as date) = dt.id_tiempo
    )
SELECT
    *
FROM
    alquileres
