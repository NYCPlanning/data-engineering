{{
    config(
        materialized='table',
        indexes=[{'columns': ['bbl'], 'unique': True}],
        tags=['pluto_enrichment']
    )
}}

-- Final transit zone assignment per BBL
-- Uses block-level for unambiguous blocks, lot-level for ambiguous ones
-- Nulls out transit zones for lots in water (outside all transit zone coverage)

WITH assignments AS (
    -- Block-level assignments for non-ambiguous blocks
    SELECT
        UNNEST(bbls) AS bbl,
        transit_zone
    FROM {{ ref('int_tz__block_to_tz_ranked') }} AS block_tz
    WHERE
        block_tz.tz_rank = 1
        -- Only assign blocks that are not ambiguous (no second-ranked transit zone)
        AND NOT EXISTS (
            SELECT 1
            FROM {{ ref('int_tz__block_to_tz_ranked') }} AS ambiguous
            WHERE
                ambiguous.id = block_tz.id
                AND ambiguous.tz_rank = 2
                AND ambiguous.pct_covered > 10
        )

    UNION ALL

    -- Lot-level assignments for ambiguous blocks
    SELECT
        bbls[1] AS bbl,
        transit_zone
    FROM {{ ref('int_tz__bbl_to_tz_ranked') }}
    WHERE tz_rank = 1
),

lots_in_coverage AS (
    SELECT p.bbl
    FROM {{ target.schema }}.pluto AS p
    INNER JOIN {{ ref('int_tz__union_coverage') }} AS tz
        ON ST_INTERSECTS(p.geom, tz.geom)
)

SELECT
    a.bbl,
    CASE
        WHEN lic.bbl IS NOT NULL THEN a.transit_zone
        ELSE NULL
    END AS trnstzone
FROM assignments AS a
LEFT JOIN lots_in_coverage AS lic ON a.bbl = lic.bbl
