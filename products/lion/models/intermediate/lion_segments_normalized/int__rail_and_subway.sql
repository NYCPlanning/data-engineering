{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']}
    ]
) }}
{%- 
    for source_layer in [
        "dcp_cscl_rail",
        "dcp_cscl_subway",
    ] 
-%}
    SELECT
        boro.boroughcode,
        source.segmentid,
        source.segment_seqnum,
        '1' AS feature_type_code,
        convert_level_code(from_level_code) AS from_level_code,
        convert_level_code(to_level_code) AS to_level_code,
        'U' AS segment_type,
        source.row_type AS right_of_way_type,
        source.legacy_segmentid,
        source.geom,
        source.shape_length,
        substring('{{ source_layer }}', 10) AS source_feature_type,
        source.row_type NOT IN ('1', '8') AS include_in_geosupport_lion,
        TRUE AS include_in_bytes_lion
    FROM {{ source("recipe_sources", source_layer) }} AS source
    LEFT JOIN {{ ref("int__streetcode_and_facecode") }} AS boro ON source.segmentid = boro.segmentid
    {% if not loop.last -%}
        UNION ALL
    {%- endif %}
{% endfor %}
