WITH arterial_highways_raw AS (
    SELECT * FROM {{ source('recipe_sources', 'dcm_arterial_highways') }}
)

SELECT
    name,
    ST_Multi(ST_Union(wkb_geometry)) AS wkb_geometry
FROM arterial_highways_raw
WHERE source = 'Appendix H'
GROUP BY name
