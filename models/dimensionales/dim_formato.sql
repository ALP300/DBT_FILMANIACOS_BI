-- models/dim/dim_formato.sql
{{ config(
    materialized='table',
    post_hook=[
        "ALTER TABLE {{ this }} DROP CONSTRAINT IF EXISTS pk_dim_formato;",
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='pk_dim_formato') THEN ALTER TABLE {{ this }} ADD CONSTRAINT pk_dim_formato PRIMARY KEY (id_formato); END IF; END $$;",
        "CREATE INDEX IF NOT EXISTS idx_dim_formato_tipo ON {{ this }} (tipo_formato);"
    ]
) }}
SELECT
    category_id AS id_formato,
    name AS tipo_formato
FROM
    category
