{{ config(
    materialized = 'table',
) }}
WITH dev AS (
    SELECT * FROM {{ ref('saf_abcegnpx_generic') }}
),
prod AS (
    SELECT * FROM {{ source('production_outputs', 'saf_abcegnpx_generic') }}
),
combined AS (
    SELECT
        'dev' AS source,
        *,
        md5(cast(dev AS TEXT)) AS row_hash
    FROM dev
    UNION ALL
    SELECT
        'prod' AS source,
        *,
        md5(cast(prod AS TEXT)) AS row_hash
    FROM prod
),
counts AS (
    SELECT
        *,
        count(*) OVER (PARTITION BY row_hash) AS match_count,
        count(CASE WHEN source = 'dev' THEN 1 END) OVER (PARTITION BY row_hash) AS dev_count,
        count(CASE WHEN source = 'prod' THEN 1 END) OVER (PARTITION BY row_hash) AS prod_count
    FROM combined
)
SELECT * FROM counts
WHERE dev_count <> prod_count
ORDER BY counts.boroughcode, counts.face_code, counts.segment_seqnum, source
