{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['lionkey']},
    ]
) }}
WITH saf AS (
    SELECT * FROM {{ ref('int__saf_segments') }}
    WHERE NOT rpl_flag
),
priorities AS (
    SELECT * FROM {{ ref('seed_saf_priority') }}
),
prioritized AS (
    SELECT
        saf.segment_lionkey AS lionkey,
        saf.saftype,
        row_number() OVER (
            PARTITION BY saf.segment_lionkey
            -- very specific fields have priority, as specified in seed table
            -- beyond that, order by source and row number
            ORDER BY p.priority, saf.saf_ogc_fid
        ) AS row_number
    FROM saf
    LEFT JOIN {{ ref('seed_saf_priority') }} AS p ON saf.saftype = p.saftype
)
SELECT
    lionkey,
    CASE
        -- The SAFTYPE "F" in CSCL is a "D" in LION/Geosupport
        -- Future exports may or may not expect this mapping
        WHEN saftype = 'F' THEN 'D'
        ELSE saftype
    END AS special_address_flag
FROM prioritized
WHERE row_number = 1
