{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
      {'columns': ['midpoint'], 'type': 'gist'},
      {'columns': ['segmentid']}
    ]
) }}

WITH seqnum AS (
    SELECT * FROM {{ ref("int__nonstreetfeature_seqnum") }}
),

street_and_facecode AS (
    SELECT * FROM {{ ref("int__streetcode_and_facecode") }}
),
-- TODO join centerline here to get stuff like twisted_parity_flag

primary_segments AS (
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
            source.segmentid,
            -- TODO all these if elses are a little inelegant, this should be reworked
            {% if source_layer == 'dcp_cscl_centerline' -%}
                -- there's only one row where this actually makes a difference. Will report to GR
                source.boroughcode,
            {% else -%}
                NULL AS boroughcode,
            {% endif -%}
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
        {% if source_layer == 'dcp_cscl_nonstreetfeatures' -%} 
            LEFT JOIN seqnum ON source.segmentid = seqnum.segmentid
        {%- endif -%}
        {% if not loop.last -%}
            UNION ALL
        {% endif -%}
    {%- endfor %}
),

segment_attributes AS (
    SELECT
        primary_segments.segmentid,
        COALESCE(primary_segments.boroughcode, street_and_facecode.boroughcode) AS boroughcode,
        primary_segments.legacy_segmentid,
        primary_segments.from_level_code,
        primary_segments.to_level_code,
        primary_segments.segment_seqnum,
        primary_segments.geom,
        primary_segments.midpoint,
        primary_segments.start_point,
        primary_segments.end_point,
        primary_segments.shape_length,
        street_and_facecode.face_code,
        street_and_facecode.five_digit_street_code,
        street_and_facecode.lgc1,
        street_and_facecode.lgc2,
        street_and_facecode.lgc3,
        street_and_facecode.lgc4,
        street_and_facecode.lgc5,
        street_and_facecode.lgc6,
        street_and_facecode.lgc7,
        street_and_facecode.lgc8,
        street_and_facecode.lgc9,
        street_and_facecode.boe_lgc_pointer::CHAR(1),
        primary_segments.feature_type,
        primary_segments.source_table,
        primary_segments.include_in_geosupport_lion,
        primary_segments.include_in_bytes_lion
    FROM primary_segments
    LEFT JOIN street_and_facecode ON primary_segments.segmentid = street_and_facecode.segmentid
)

SELECT * FROM segment_attributes
