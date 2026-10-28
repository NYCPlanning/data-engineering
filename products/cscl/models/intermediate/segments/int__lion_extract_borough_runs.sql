-- Which per-borough LION extract runs process each source feature. The legacy ETL extracts
-- LION one borough at a time; each run reads every feature that intersects the borough
-- polygon buffered by BoroughBufferDistance (2 ft; ExtractorClass.GetBufferedBoroughPolygon,
-- ExtractFeatures). A feature on a borough boundary is read by both runs - it is written to
-- the file of its own borough only, but its proto-segments are initialized (and can take a
-- sequence number) in every run that reads it.
--
-- Only the features the non-street-feature sequence counter (int__nonstreetfeature_seqnum)
-- needs: every NonStreetFeature, and every other feature with a proto-segment whose
-- implicit feature class is NonStreetFeature (FEATURE_TYPE 3/4/7/8).
{{ config(
    materialized = 'table'
) }}
WITH boroughs AS (
    SELECT
        borocode,
        ST_BUFFER(geom, {{ var('lion_extract_borough_buffer_ft') }}) AS geom
    FROM {{ ref('stg__borough') }}
),
nsf_proto_parents AS (
    SELECT DISTINCT segmentid
    FROM {{ ref('stg__altsegmentdata_proto') }}
    WHERE saftype IS NULL AND feature_type_code IN ('3', '4', '7', '8')
),
-- class_rank: the order LIONExtractorClass.ExtractBorough extracts the feature classes in
features AS (
    SELECT
        1 AS class_rank,
        source_table,
        segmentid,
        ogc_fid,
        globalid,
        geom
    FROM {{ ref('stg__centerline') }}
    WHERE segmentid IN (SELECT nsf_proto_parents.segmentid FROM nsf_proto_parents)
    UNION ALL
    SELECT
        2 AS class_rank,
        source_table,
        segmentid,
        ogc_fid,
        globalid,
        geom
    FROM {{ ref('stg__nonstreetfeatures') }}
    UNION ALL
    SELECT
        CASE source_table WHEN 'rail' THEN 3 ELSE 5 END AS class_rank,
        source_table,
        segmentid,
        ogc_fid,
        globalid,
        geom
    FROM {{ ref('stg__rail_and_subway') }}
    WHERE segmentid IN (SELECT nsf_proto_parents.segmentid FROM nsf_proto_parents)
    UNION ALL
    SELECT
        4 AS class_rank,
        source_table,
        segmentid,
        ogc_fid,
        globalid,
        geom
    FROM {{ ref('stg__shoreline') }}
    WHERE segmentid IN (SELECT nsf_proto_parents.segmentid FROM nsf_proto_parents)
)
SELECT
    features.class_rank,
    features.source_table,
    features.segmentid,
    features.ogc_fid,
    features.globalid,
    boroughs.borocode AS run_borocode
FROM features
INNER JOIN boroughs ON ST_INTERSECTS(features.geom, boroughs.geom)
