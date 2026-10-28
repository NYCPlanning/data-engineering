SELECT
    {{ select_fields_from_seed('text_formatting__thinfire_dat') }},
    -- the file's record order (recipe.yml order_by); the layout ignores it
    source_objectid
FROM {{ ref('thinfire_by_field_unformatted') }}
