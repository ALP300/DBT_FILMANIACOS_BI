with films as (
    select * from {{ ref('stg_film') }}
),

languages as (
    select * from {{ source('dvdrental', 'language') }}
),

categories as (
    select 
        fc.film_id,
        c.name as category_name
    from {{ source('dvdrental', 'film_category') }} fc
    left join {{ source('dvdrental', 'category') }} c on fc.category_id = c.category_id
),

final as (
    select
        f.film_id,
        f.title,
        f.description,
        f.release_year,
        f.rental_duration,
        f.rental_rate,
        f.length,
        f.replacement_cost,
        f.rating,
        f.special_features,
        
        -- Language
        l.name as language,
        
        -- Category
        c.category_name,
        
        f.last_update
    from films f
    left join languages l on f.language_id = l.language_id
    left join categories c on f.film_id = c.film_id
)

select * from final