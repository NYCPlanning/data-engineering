{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
      {'columns': ['midpoint'], 'type': 'gist'},
      {'columns': ['segmentid']}
    ]
) }}
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
        {% if source_layer == 'dcp_cscl_centerline' -%}
            -- there's only one row where this actually makes a difference. Will report to GR
            source.boroughcode,
        {% else -%}
            boro.boroughcode,
        {% endif -%}
        source.segmentid,
        {% if source_layer == 'dcp_cscl_shoreline' -%} 
            NULL AS legacy_segmentid,
            NULL AS from_level_code,
            NULL AS to_level_code,
            source.segment_seqnum,
        {% elif source_layer == 'dcp_cscl_nonstreetfeatures' -%}
            source.legacy_segmentid,
            NULL AS from_level_code,
            NULL AS to_level_code,
            seqnum.segment_seqnum,
        {% else -%}
            source.legacy_segmentid,
            source.from_level_code,
            source.to_level_code,
            source.segment_seqnum,
        {% endif -%}
        ST_LINEMERGE(source.geom) AS geom,
        ST_LINEINTERPOLATEPOINT(ST_LINEMERGE(source.geom), 0.5) AS midpoint,
        ST_STARTPOINT(ST_LINEMERGE(source.geom)) AS start_point,
        ST_ENDPOINT(ST_LINEMERGE(source.geom)) AS end_point,
        source.shape_length,
        SUBSTRING('{{ source_layer }}', 10) AS feature_type,
        '{{ source_layer }}' AS source_table,
        {% if source_layer == 'dcp_cscl_centerline' -%} 
            (rwjurisdiction IS DISTINCT FROM '3' OR status = '2') AND rw_type <> 8 AS include_in_geosupport_lion,
            (rwjurisdiction IS DISTINCT FROM '3' OR status = '2') AS include_in_bytes_lion
        {% elif source_layer == 'dcp_cscl_rail' or source_layer == 'dcp_cscl_subway' -%}
            row_type NOT IN ('1', '8') AS include_in_geosupport_lion,
            TRUE AS include_in_bytes_lion
        {% else -%}
            TRUE AS include_in_geosupport_lion,
            TRUE AS include_in_bytes_lion
        {% endif -%}
    FROM {{ source("recipe_sources", source_layer) }} AS source
    LEFT JOIN {{ ref("int__streetcode_and_facecode") }} AS boro ON source.segmentid = boro.segmentid
    {% if source_layer == 'dcp_cscl_nonstreetfeatures' -%} 
        LEFT JOIN {{ ref("int__nonstreetfeature_seqnum") }} AS seqnum ON source.segmentid = seqnum.segmentid
    {% endif %}
    {% if not loop.last -%}
        UNION ALL
    {%- endif %}
{% endfor %}
