SELECT
    'forever_wild' AS variable_type,
    gispropnum || '-' || propertyna AS variable_id,
    wkb_geometry AS raw_geom,
    NULL AS buffer
FROM {{ source("recipe_sources", "dpr_forever_wild") }}
