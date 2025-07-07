{%- 
    for source_layer in [
        "dcp_cscl_centerline",
        "dcp_cscl_shoreline",
        "dcp_cscl_railroads",
        "dcp_cscl_subways",
        "dcp_cscl_nonstreetfeatures",
    ] 
-%}
    SELECT
        segmentid,
        ST_LINEMERGE(geom) AS geom
    FROM {{ source("recipe_sources", source_layer) }}
    {% if not loop.last -%}
        UNION ALL
    {%- endif %}
{% endfor %}
