/*
TODO - this logic is a little duplicated at the moment
    with int__protosegments and int__segments. But it also seems
    annoying to have multiple segment intermediate models as we
    add face code, seqnum, etc one at a time.

    For now, slightly duplicated logic. But we should revisit

    Also TODO - get this actually matching prod
*/
{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['globalid']},
    ]
) }}
WITH nsf AS (
    SELECT * FROM {{ ref('stg__nonstreetfeatures') }}
),
facecode AS (
    SELECT * FROM {{ ref('int__streetcode_and_facecode') }}
),
proto AS (
    SELECT * FROM {{ ref('stg__altsegmentdata_proto') }}
),
proto_facecode AS (
    SELECT * FROM {{ ref('int__b7sc_codes') }}
),
other_segments AS (
    SELECT
        segmentid,
        segment_seqnum
    FROM {{ ref('stg__centerline') }}
    UNION ALL
    SELECT
        segmentid,
        segment_seqnum
    FROM {{ ref('stg__shoreline') }}
    UNION ALL
    SELECT
        segmentid,
        segment_seqnum
    FROM {{ ref('stg__rail_and_subway') }}
),
all_nsf_segments AS (
    SELECT
        source_table,
        nsf.globalid,
        nsf.segmentid,
        nsf.ogc_fid,
        facecode.boroughcode,
        facecode.face_code
    FROM nsf
    INNER JOIN facecode ON nsf.segmentid = facecode.segmentid
    UNION ALL
    SELECT
        proto.source_table,
        proto.globalid,
        proto.segmentid,
        proto.ogc_fid,
        proto.borough AS boroughcode,
        proto_facecode.feature_facecode AS face_code
    FROM proto
    INNER JOIN proto_facecode ON proto.b7sc = proto_facecode.b7sc
),
nsf_bfcs AS (
    SELECT
        boroughcode,
        face_code,
        COUNT(*) AS count
    FROM all_nsf_segments
    GROUP BY boroughcode, face_code
),
potential_collisions AS (
    SELECT
        facecode.boroughcode,
        facecode.face_code,
        other_segments.segment_seqnum::INT
    FROM other_segments
    INNER JOIN facecode ON other_segments.segmentid = facecode.segmentid
    INNER JOIN nsf_bfcs ON facecode.boroughcode = nsf_bfcs.boroughcode AND facecode.face_code = nsf_bfcs.face_code
),
collision_bfcs AS (
    SELECT
        boroughcode,
        face_code,
        COUNT(*) AS count
    FROM potential_collisions
    GROUP BY boroughcode, face_code
),
allowed_values AS (
    SELECT
        bfc.boroughcode,
        bfc.face_code,
        ROW_NUMBER() OVER (
            PARTITION BY bfc.boroughcode, bfc.face_code
            ORDER BY gs.seqnum
        ) AS rank,
        gs.seqnum
    FROM collision_bfcs AS bfc
    CROSS JOIN
        GENERATE_SERIES(
            -- upper bound of how many seqnums we need to generate is max rows per bfc in nsf and max rows per bfc in non-nsf
            1, (SELECT MAX(cbfc.count) FROM collision_bfcs AS cbfc) + (SELECT MAX(nbfc.count) FROM nsf_bfcs AS nbfc)
        ) AS gs (seqnum)
    WHERE NOT EXISTS (
        SELECT 1
        FROM potential_collisions AS s
        WHERE
            s.boroughcode = bfc.boroughcode
            AND s.face_code = bfc.face_code
            AND s.segment_seqnum = gs.seqnum
    )
),
ranked_nsfs AS (
    SELECT
        source_table,
        globalid,
        segmentid,
        boroughcode,
        face_code,
        ROW_NUMBER() OVER (
            PARTITION BY boroughcode, face_code
            ORDER BY source_table DESC, ogc_fid ASC
        ) AS rank
    FROM all_nsf_segments
)
SELECT
    ranked_nsfs.source_table,
    ranked_nsfs.globalid,
    ranked_nsfs.segmentid,
    ranked_nsfs.boroughcode,
    ranked_nsfs.face_code,
    LPAD(COALESCE(allowed_values.seqnum, ranked_nsfs.rank)::TEXT, 5, '0') AS segment_seqnum
FROM ranked_nsfs
LEFT JOIN
    allowed_values
    ON
        ranked_nsfs.boroughcode = allowed_values.boroughcode
        AND ranked_nsfs.face_code = allowed_values.face_code
        AND ranked_nsfs.rank = allowed_values.rank
