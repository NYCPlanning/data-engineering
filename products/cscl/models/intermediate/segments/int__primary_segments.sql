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

linearized AS (
    SELECT
        *,
        LINEARIZE(
            raw_geom, 
            CASE
                WHEN feature_type = 'centerline' THEN 0.00025 
                ELSE 0.01 
            END
        ) AS geom
    FROM primary_segments
),

segment_attributes AS (
    SELECT
        COALESCE(segments.boroughcode, street_and_facecode.boroughcode) AS boroughcode,
        street_and_facecode.face_code,
        segments.segment_seqnum,
        segments.segmentid,
        segments.from_level_code,
        segments.to_level_code,
        segments.legacy_segmentid,
        segments.feature_type_code,
        segments.geom,
        segments.raw_geom,
        ST_LINEINTERPOLATEPOINT(segments.geom, 0.5) AS midpoint,
        ST_STARTPOINT(segments.geom) AS start_point,
        ST_ENDPOINT(segments.geom) AS end_point,
        segments.shape_length,
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
        segments.feature_type,
        segments.feature_type AS primary_feature_type,
        feature_type_codes.description AS feature_type_description,
        segments.source_table,
        segments.globalid
    FROM linearized AS segments
    LEFT JOIN street_and_facecode ON segments.segmentid = street_and_facecode.segmentid
    LEFT JOIN feature_type_codes ON segments.feature_type_code = feature_type_codes.code
)

SELECT * FROM segment_attributes
WHERE face_code IS NOT NULL -- TODO error report for this and maybe refactor to get this in a more logical place
