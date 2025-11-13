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
                -- there's only one row where this actually makes a difference. Will report to GR
                source.boroughcode,
            {% else -%}
                NULL AS boroughcode,
            {% endif -%}
            {% if source_layer == 'stg__shoreline' -%} 
                NULL AS legacy_segmentid,
                NULL AS from_level_code,
                NULL AS to_level_code,
                source.segment_seqnum,
            {% elif source_layer == 'stg__nonstreetfeatures' -%}
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
            source.feature_type_code,
            ST_LINEMERGE(source.geom) AS geom,
            ST_LINEINTERPOLATEPOINT(ST_LINEMERGE(source.geom), 0.5) AS midpoint,
            ST_STARTPOINT(ST_LINEMERGE(source.geom)) AS start_point,
            ST_ENDPOINT(ST_LINEMERGE(source.geom)) AS end_point,
            source.shape_length,
            source.feature_type,
            source.source_table
        FROM {{ ref(source_layer) }} AS source
        {% if source_layer == 'stg__nonstreetfeatures' -%} 
            LEFT JOIN seqnum
                ON
                    seqnum.source_table = 'nonstreetfeatures'
                    AND source.segmentid = seqnum.unique_id
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
        primary_segments.geom,
        primary_segments.midpoint,
        primary_segments.start_point,
        primary_segments.end_point,
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
        feature_type_codes.description AS feature_type_description,
        primary_segments.source_table
    FROM primary_segments
    LEFT JOIN street_and_facecode ON primary_segments.segmentid = street_and_facecode.segmentid
    LEFT JOIN feature_type_codes ON primary_segments.feature_type_code = feature_type_codes.code
)

SELECT * FROM segment_attributes
WHERE face_code IS NOT NULL -- TODO error report for this and maybe refactor to get this in a more logical place
