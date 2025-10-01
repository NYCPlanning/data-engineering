{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}
WITH nsf AS (
    SELECT * FROM {{ ref('stg__nonstreetfeatures') }}
),
facecode AS (
    SELECT * FROM {{ ref('int__streetcode_and_facecode') }}
)
SELECT
    nsf.segmentid,
    facecode.boroughcode,
    facecode.face_code,
    LPAD(ROW_NUMBER() OVER (
        PARTITION BY facecode.boroughcode, facecode.face_code
        ORDER BY nsf.ogc_fid
    )::text, 5, '0') AS segment_seqnum
FROM nsf
INNER JOIN facecode ON nsf.segmentid = facecode.segmentid
