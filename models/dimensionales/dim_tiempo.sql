-- models/dim/dim_tiempo.sql
{{ config(
    materialized='table',
    post_hook=[
        "ALTER TABLE {{ this }} DROP CONSTRAINT IF EXISTS pk_dim_tiempo;",
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='pk_dim_tiempo') THEN ALTER TABLE {{ this }} ADD CONSTRAINT pk_dim_tiempo PRIMARY KEY (id_tiempo); END IF; END $$;",
        "CREATE INDEX IF NOT EXISTS idx_dim_tiempo_fecha ON {{ this }} (fecha);"
    ]
) }}
WITH dates AS (
    SELECT DISTINCT cast(rental_date AS date) AS id_tiempo
    FROM rental
)
SELECT
    id_tiempo,
    id_tiempo AS fecha,
    EXTRACT(DAY FROM id_tiempo) AS dia,
    EXTRACT(MONTH FROM id_tiempo) AS mes,
    EXTRACT(YEAR FROM id_tiempo) AS año,
    EXTRACT(QUARTER FROM id_tiempo) AS trimestre
FROM dates
