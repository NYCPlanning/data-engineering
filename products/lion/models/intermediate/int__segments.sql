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
        "int__centerline",
        "int__shoreline",
        "int__rail_and_subway",
        "int__nonstreetfeatures",
    ] 
-%}
    SELECT
        boroughcode,
        segmentid,
        segment_seqnum,
        feature_type_code,
        curve_flag,
        from_level_code,
        to_level_code,
        segment_type,
        legacy_segmentid,
        ST_LINEMERGE(geom) AS geom,
        ST_LINEINTERPOLATEPOINT(ST_LINEMERGE(geom), 0.5) AS midpoint,
        ST_STARTPOINT(ST_LINEMERGE(geom)) AS start_point,
        ST_ENDPOINT(ST_LINEMERGE(geom)) AS end_point,
        shape_length,
        source_feature_type,
        include_in_geosupport_lion,
        include_in_bytes_lion
    FROM {{ ref(source_layer) }}
    {% if not loop.last -%}
        UNION ALL
    {%- endif %}
{% endfor %}
