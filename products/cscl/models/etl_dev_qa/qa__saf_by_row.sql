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
    	'dev' as source,
        *,
        md5(CAST(dev AS text)) AS row_hash
    FROM dev
    UNION ALL
    SELECT 
    	'prod' as source,
        *,
        md5(CAST(prod AS text)) AS row_hash
    FROM prod
),
counts AS (
    SELECT 
        *,
        COUNT(*) OVER (PARTITION BY row_hash) AS match_count,
        COUNT(CASE WHEN source = 'dev' THEN 1 END) OVER (PARTITION BY row_hash) AS dev_count,
        COUNT(CASE WHEN source = 'prod' THEN 1 END) OVER (PARTITION BY row_hash) AS prod_count
    FROM combined
)
SELECT * FROM counts
WHERE dev_count <> prod_count
ORDER BY counts.boroughcode, counts.face_code, counts.segment_seqnum, source
