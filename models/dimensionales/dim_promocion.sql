-- models/dim/dim_promocion.sql
{{ config(
    materialized='table',
    post_hook=[
        "ALTER TABLE {{ this }} DROP CONSTRAINT IF EXISTS pk_dim_promocion;",
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='pk_dim_promocion') THEN ALTER TABLE {{ this }} ADD CONSTRAINT pk_dim_promocion PRIMARY KEY (id_promocion); END IF; END $$;",
        "CREATE INDEX IF NOT EXISTS idx_dim_promocion_tipo ON {{ this }} (tipo_promocion);"
    ]
) }}
SELECT
    promotion_id AS id_promocion,
    discount_type AS tipo_promocion,
    description AS descripcion,
    start_date AS fecha_inicio,
    end_date AS fecha_fin
FROM
    promotion
