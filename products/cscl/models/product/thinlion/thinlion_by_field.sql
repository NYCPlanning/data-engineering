
SELECT
    {{ apply_text_formatting_from_seed('text_formatting__thinlion_dat') }}
FROM {{ ref("stg__atomic_polygons" ) }}

-- maybe more intermediate stuff here.
