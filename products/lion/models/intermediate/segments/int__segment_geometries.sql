{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
      {'columns': ['midpoint'], 'type': 'gist'},
      {'columns': ['segmentid']}
    ]
) }}

WITH sources AS (
{%
    for source_layer in [
        "dcp_cscl_centerline",
        "dcp_cscl_shoreline",
        "dcp_cscl_rail",
        "dcp_cscl_subway",
        "dcp_cscl_nonstreetfeatures",
    ] 
-%}
    SELECT
        segmentid,
        ST_LINEMERGE(geom) AS geom,
        shape_length,
        SUBSTRING('{{ source_layer }}', 10) AS feature_type,
        '{{ source_layer }}' AS source_table,
        to_jsonb(source) - 'geom' AS source_attrs
    FROM {{ source("recipe_sources", source_layer) }} AS source
    {% if not loop.last -%}
        UNION ALL
    {%- endif %}
{% endfor %}
)
SELECT
    segmentid,
    geom,
    ST_LINEINTERPOLATEPOINT(geom, 0.5) AS midpoint,
    ST_STARTPOINT(geom) AS start_point,
    ST_ENDPOINT(geom) AS end_point,
    shape_length,
    feature_type,
    source_table
FROM sources
/*
TODO this should go to int__lion
CASE
    WHEN feature_type = 'centerline' THEN
        (centerline.rwjurisdiction IS DISTINCT FROM '3' OR status = '2') AND centerline.rw_type <> 8
    WHEN feature_type in ('dcp_cscl_rail', 'dcp_cscl_subway') THEN
        rail.row_type NOT IN ('1', '8')
    ELSE
        TRUE
END AS include_in_geosupport_lion
CASE
    WHEN feature_type = 'centerline' THEN
        (centerline.rwjurisdiction IS DISTINCT FROM '3' OR centerline.status = '2')
    ELSE
        TRUE
END AS include_in_geosupport_lion
*/
