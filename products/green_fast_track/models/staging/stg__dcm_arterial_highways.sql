WITH arterial_highways_raw AS (
    SELECT * FROM {{ source('recipe_sources', 'dcm_arterial_highways') }}
)

SELECT
    name,
    ST_UNION(wkb_geometry) AS wkb_geometry
FROM arterial_highways_raw
WHERE source = 'Appendix H'
GROUP BY name
