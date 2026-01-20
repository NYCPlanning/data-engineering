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

feature_type_codes AS (
    SELECT * FROM {{ ref("feature_type_codes") }}
),

primary_segments AS (
    {%- 
        for source_layer in [
            "stg__centerline",
            "stg__shoreline",
            "stg__rail_and_subway",
            "stg__nonstreetfeatures",
        ] 
    -%}
        SELECT
            source.segmentid,
            -- TODO all these if elses are a little inelegant, this should be reworked
            {% if source_layer == 'stg__centerline' -%}
                source.boroughcode,
                source.segment_type,
            {% else -%}
                NULL AS boroughcode,
                'U' AS segment_type,
            {% endif -%}
            {% if source_layer == 'stg__shoreline' -%} 
                NULL::INT AS legacy_segmentid,
                NULL AS from_level_code,
                NULL AS to_level_code,
                source.segment_seqnum,
            {% elif source_layer == 'stg__nonstreetfeatures' -%}
                source.legacy_segmentid::INT,
                NULL AS from_level_code,
                NULL AS to_level_code,
                seqnum.segment_seqnum,
            {% else -%}
                source.legacy_segmentid,
                source.from_level_code,
                source.to_level_code,
                source.segment_seqnum,
            {% endif -%}
            source.feature_type_code,
            LINEARIZE(source.geom) AS geom,
            source.geom AS raw_geom,
            source.shape_length,
            source.feature_type,
            source.source_table,
            source.globalid
        FROM {{ ref(source_layer) }} AS source
        {% if source_layer == 'stg__nonstreetfeatures' -%} 
            LEFT JOIN seqnum
                ON
                    seqnum.source_table = 'nonstreetfeatures'
                    AND source.globalid = seqnum.globalid
        {%- endif -%}
        {% if not loop.last -%}
            UNION ALL
        {% endif -%}
    {%- endfor %}
),

segment_attributes AS (
    SELECT
        COALESCE(primary_segments.boroughcode, street_and_facecode.boroughcode) AS boroughcode,
        street_and_facecode.face_code,
        primary_segments.segment_seqnum,
        primary_segments.segmentid,
        primary_segments.from_level_code,
        primary_segments.to_level_code,
        primary_segments.legacy_segmentid,
        primary_segments.feature_type_code,
        primary_segments.segment_type,
        primary_segments.geom,
        primary_segments.raw_geom,
        ST_LINEINTERPOLATEPOINT(primary_segments.geom, 0.5) AS midpoint,
        ST_STARTPOINT(primary_segments.geom) AS start_point,
        ST_ENDPOINT(primary_segments.geom) AS end_point,
        primary_segments.shape_length,
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
        primary_segments.feature_type AS primary_feature_type,
        feature_type_codes.description AS feature_type_description,
        primary_segments.source_table,
        primary_segments.globalid
    FROM primary_segments
    LEFT JOIN street_and_facecode ON primary_segments.segmentid = street_and_facecode.segmentid
    LEFT JOIN feature_type_codes ON primary_segments.feature_type_code = feature_type_codes.code
)

SELECT * FROM segment_attributes
WHERE face_code IS NOT NULL -- TODO error report for this and maybe refactor to get this in a more logical place
