-- models/dim/dim_pelicula.sql
{{ config(
    materialized='table',
    post_hook=[
    "ALTER TABLE {{ this }} DROP CONSTRAINT IF EXISTS pk_dim_pelicula;",
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='pk_dim_pelicula') THEN ALTER TABLE {{ this }} ADD CONSTRAINT pk_dim_pelicula PRIMARY KEY (id_pelicula); END IF; END $$;",
        "CREATE INDEX IF NOT EXISTS idx_dim_pelicula_title ON {{ this }} (titulo);"
    ]
) }}
SELECT
    film_id AS id_pelicula,
    title AS titulo,
    description AS descripcion,
    release_year AS año_lanzamiento,
    language_id AS id_idioma,
    rental_duration AS duracion_renta,
    rental_rate AS tarifa_renta,
    length AS duracion,
    replacement_cost AS costo_reemplazo,
    rating,
    special_features,
    last_update
FROM
    film
