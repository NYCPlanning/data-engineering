-- The rezoning tracker has no BBL or geometry - it's keyed by rezoning_area (13 named,
-- neighborhood-scale study areas) x year x commitment. seeds/rezoning_areas.csv is a
-- hand-built crosswalk from rezoning_area to the NTA(s) it covers (unmatched areas -
-- citywide rollups, and a few with no clear NTA correspondence - are left out of the
-- seed with a null nta_name and dropped here). A bbl's NTA (same join path as
-- int__lift_dri: pluto.boroct2020 -> ct2020.boroct2020) decides which rezoning area's
-- commitments apply to it.
WITH crosswalk AS (
    SELECT rezoning_area, nta_name
    FROM {{ ref('rezoning_areas') }}
    WHERE nta_name IS NOT NULL
),

latest_status AS (
    SELECT * FROM {{ ref('int__rezoning_commitments_latest') }}
),

tracker_by_nta AS (
    SELECT
        crosswalk.nta_name,
        latest_status.commitment_id,
        latest_status.commitment_title,
        latest_status.commitment_stage
    FROM latest_status
    INNER JOIN crosswalk ON latest_status.rezoning_area = crosswalk.rezoning_area
),

lift AS (
    SELECT bbl FROM {{ ref('stg__lift_csv') }}
),

pluto AS (
    SELECT bbl, boroct2020 FROM {{ ref('stg__pluto') }}
),

ct2020 AS (
    SELECT boroct2020, ntaname FROM {{ ref('stg__ct2020') }}
),

lift_nta AS (
    SELECT lift.bbl, ct2020.ntaname
    FROM lift
    INNER JOIN pluto ON lift.bbl = pluto.bbl
    INNER JOIN ct2020 ON pluto.boroct2020 = ct2020.boroct2020
),

matched AS (
    SELECT
        lift_nta.bbl,
        tracker_by_nta.commitment_id,
        tracker_by_nta.commitment_title,
        tracker_by_nta.commitment_stage
    FROM lift_nta
    INNER JOIN tracker_by_nta ON lift_nta.ntaname = tracker_by_nta.nta_name
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
