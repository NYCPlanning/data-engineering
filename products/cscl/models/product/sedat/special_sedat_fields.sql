SELECT
    {{ select_fields_from_seed('text_formatting__sedat') }},
    -- the file's record order (recipe.yml order_by); the layout ignores it
    source_objectid
FROM {{ ref("int__special_sedat") }}
WHERE street_name IS NOT NULL
