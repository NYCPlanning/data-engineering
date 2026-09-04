WITH arterial_highways_raw AS (
    SELECT * FROM {{ source('recipe_sources', 'dcm_arterial_highways') }}
)

SELECT
    name,
    st_multi({{ dcp_st_transform('st_union_agg(wkb_geometry)', 2263) }}) AS wkb_geometry
FROM arterial_highways_raw
WHERE source = 'Appendix H'
GROUP BY name
