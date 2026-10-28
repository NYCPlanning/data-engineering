-- Segment sequence numbers (LION L3) for NonStreetFeature records and for proto-segments whose
-- implicit feature class is NonStreetFeature (FEATURE_TYPE 3/4/7/8), numbered the way the
-- legacy ETL numbers them (ETL spec 2.5.1: assigned "on the fly").
--
-- The legacy GetSequenceNumber (NonStreetFeatureExtractFile.cs:280, CenterlineExtractFile.cs:1235)
-- is a counter per (borough, face code): every call adds one and returns the new value. A
-- proto-segment whose implicit class isn't NonStreetFeature still takes a number but keeps its
-- own ALT_SEGMENT_SEQNUM. The counter is kept across the borough runs of one extract session.
-- So a number is the count of calls on its key, in processing order, up to and including its own.
--
-- Processing order: borough runs (var lion_seqnum_extract_sessions), then feature class
-- (class_rank), then source OBJECTID, then each feature's proto-segments in OBJECTID order.
-- A run reads every feature within 2 ft of its borough (int__lion_extract_borough_runs).
-- The calls, per run and feature:
--   1. NonStreetFeature written in this run (its borough is the run's): one call on its own key.
--   2. Each NSF-implicit proto-segment, whenever its parent is read, written or not: one call at
--      proto initialization (CenterlineExtractFile.GetSpecialRowValues, :3524). Under a
--      NonStreetFeature parent the borough of that key is a stale field: the borough of the
--      previous proto-segment of the parent, else the parent's own; elsewhere it is the
--      proto-segment's own borough. The face code is the proto-segment's.
--      Outside the NonStreetFeature extractor this is the number written.
--   3. Under a NonStreetFeature parent, each proto-segment written in this run (any implicit
--      class): a second call when its L3 field is written (NonStreetFeatureExtractFile.cs:108).
--      This is the number written for an NSF-implicit one.
{{ config(
    materialized = 'table'
) }}
{%- set runs = [] -%}
{%- for session in var('lion_seqnum_extract_sessions') -%}
    {%- set session_number = loop.index -%}
    {%- for borocode in session -%}
        {%- do runs.append((session_number, loop.index, borocode)) -%}
    {%- endfor -%}
{%- endfor %}
WITH runs AS (
    SELECT run_values.* FROM (
        VALUES
        {%- for session_number, run_order, borocode in runs %}
            ({{ session_number }}, {{ run_order }}, '{{ borocode }}'){{ "," if not loop.last }}
        {%- endfor %}
    ) AS run_values (session_number, run_order, borocode)
),
features AS (
    SELECT * FROM {{ ref('int__lion_extract_borough_runs') }}
),
facecode AS (
    SELECT * FROM {{ ref('int__streetcode_and_facecode') }}
),
proto_facecode AS (
    SELECT * FROM {{ ref('stg__facecode_and_featurename_principal') }}
),
-- the proto-segments legacy reads for a feature (LIONExtractorClass.GetProtoSegments)
all_protos AS (
    SELECT
        proto.globalid,
        proto.segmentid,
        proto.ogc_fid,
        proto.borough,
        proto.b7sc,
        proto.feature_type_code IN ('3', '4', '7', '8') AS is_nsf_implicit,
        LAG(proto.borough) OVER (PARTITION BY proto.segmentid ORDER BY proto.ogc_fid) AS previous_proto_borough
    FROM {{ ref('stg__altsegmentdata_proto') }} AS proto
    WHERE proto.saftype IS NULL
),
protos AS (
    SELECT
        all_protos.*,
        proto_facecode.face_code
    FROM all_protos
    INNER JOIN proto_facecode ON all_protos.b7sc = proto_facecode.b7sc
),
feature_runs AS (
    SELECT
        runs.session_number,
        runs.run_order,
        runs.borocode AS run_borocode,
        features.class_rank,
        features.source_table,
        features.segmentid,
        features.ogc_fid,
        features.globalid,
        facecode.boroughcode,
        facecode.face_code
    FROM features
    INNER JOIN runs ON features.run_borocode = runs.borocode
    LEFT JOIN facecode ON features.segmentid = facecode.segmentid
),
calls AS (
    -- 1. NonStreetFeature written in this run
    SELECT
        session_number,
        run_order,
        class_rank,
        ogc_fid AS feature_ogc_fid,
        0 AS proto_ogc_fid,
        0 AS step,
        boroughcode AS key_borough,
        face_code AS key_face_code,
        'nonstreetfeatures' AS source_table,
        globalid,
        segmentid,
        ogc_fid
    FROM feature_runs
    WHERE source_table = 'nonstreetfeatures' AND boroughcode = run_borocode AND face_code IS NOT NULL
    UNION ALL
    -- 2. NSF-implicit proto-segment initialization, written or not
    SELECT
        feature_runs.session_number,
        feature_runs.run_order,
        feature_runs.class_rank,
        feature_runs.ogc_fid AS feature_ogc_fid,
        protos.ogc_fid AS proto_ogc_fid,
        1 AS step,
        CASE
            WHEN feature_runs.source_table = 'nonstreetfeatures'
                THEN COALESCE(protos.previous_proto_borough, feature_runs.boroughcode)
            ELSE protos.borough
        END AS key_borough,
        protos.face_code AS key_face_code,
        'altsegmentdata' AS source_table,
        CASE
            WHEN feature_runs.source_table != 'nonstreetfeatures' AND protos.borough = feature_runs.run_borocode
                THEN protos.globalid
        END AS globalid,
        protos.segmentid,
        protos.ogc_fid
    FROM feature_runs
    INNER JOIN protos ON feature_runs.segmentid = protos.segmentid
    WHERE protos.is_nsf_implicit
    UNION ALL
    -- 3. proto-segment of a NonStreetFeature written in this run
    SELECT
        feature_runs.session_number,
        feature_runs.run_order,
        feature_runs.class_rank,
        feature_runs.ogc_fid AS feature_ogc_fid,
        protos.ogc_fid AS proto_ogc_fid,
        2 AS step,
        protos.borough AS key_borough,
        protos.face_code AS key_face_code,
        'altsegmentdata' AS source_table,
        CASE WHEN protos.is_nsf_implicit THEN protos.globalid END AS globalid,
        protos.segmentid,
        protos.ogc_fid
    FROM feature_runs
    INNER JOIN protos ON feature_runs.segmentid = protos.segmentid
    WHERE feature_runs.source_table = 'nonstreetfeatures' AND protos.borough = feature_runs.run_borocode
),
numbered AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY session_number, key_borough, key_face_code
            ORDER BY run_order, class_rank, feature_ogc_fid, proto_ogc_fid, step
        ) AS seqnum
    FROM calls
)
SELECT
    source_table,
    globalid,
    segmentid,
    ogc_fid,
    key_borough AS boroughcode,
    key_face_code AS face_code,
    LPAD(seqnum::TEXT, 5, '0') AS segment_seqnum
FROM numbered
WHERE globalid IS NOT NULL
