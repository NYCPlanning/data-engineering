{%- 
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
        st_linemerge(geom) AS geom
    FROM {{ source("recipe_sources", source_layer) }}
    {% if not loop.last -%}
        UNION ALL
    {%- endif %}
{% endfor %}
