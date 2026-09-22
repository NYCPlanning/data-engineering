-- The rezoning tracker has no BBL or geometry - it's keyed by rezoning_area (13 named,
-- neighborhood-scale study areas) x year x commitment. int__lift_rezoning_areas does the
-- geo-matching (bbl -> rezoning_area, via seeds/rezoning_areas.csv's ULURP crosswalk and
-- dcp_zoningmapamendments' mapped geometry); this model just joins that against the
-- tracker's commitments and rolls up to one row per bbl. This replaced an earlier
-- NTA-based join (bbl -> census tract -> NTA -> rezoning_area by name) that was too
-- coarse - NTAs are much larger than the actual rezoning boundaries.
WITH area_bbls AS (
    SELECT * FROM {{ ref('int__lift_rezoning_areas') }}
),

latest_status AS (
    SELECT * FROM {{ ref('int__rezoning_commitments_latest') }}
),

matched AS (
    SELECT DISTINCT
        area_bbls.bbl,
        latest_status.commitment_id,
        latest_status.commitment_title,
        latest_status.commitment_stage
    FROM area_bbls
    INNER JOIN latest_status ON area_bbls.rezoning_area = latest_status.rezoning_area
),

final AS (
    SELECT
        bbl,
        COUNT(DISTINCT commitment_id) AS n_commitments,
        COUNT(DISTINCT commitment_id) FILTER (
            WHERE LOWER(commitment_stage) IN (
                'done', 'completed', 'completed and ongoing', 'done with ongoing work'
            )
        ) AS n_done,
        COUNT(DISTINCT commitment_id) FILTER (
            WHERE LOWER(commitment_stage) = 'in progress'
        ) AS n_in_progress,
        -- "id: title (stage)" per commitment, for tracing the counts above back to
        -- which specific commitments they refer to. commitment_id is artificial (see
        -- stg__hpd_rezoning_tracker) - stable within a build, not a source-provided key.
        STRING_AGG(
            commitment_id || ': ' || commitment_title || ' (' || COALESCE(commitment_stage, 'Unknown') || ')',
            ' | '
            ORDER BY commitment_id
        ) AS commitment_detail
    FROM matched
    GROUP BY bbl
)

SELECT * FROM final
