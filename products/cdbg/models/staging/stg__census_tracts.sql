SELECT
    geoid,
    borocode::integer AS borough_code,
    boroname AS borough_name,
    ntaname,
    nta2020,
    boroct2020 AS bct2020_source,
    borocode::integer || ct2020 as bct2020,
    ct2020,
    ctlabel,
    wkb_geometry AS geom
FROM {{ source("recipe_sources", "dcp_ct2020_wi") }}
