-- One row per commitment_id (current status) - the tracker is a yearly progress
-- snapshot, so the same commitment recurs across years and should only count once.
-- Shared by int__lift_rezoning_tracker (per-bbl rollup) and the rezoning_commitments
-- product export (full commitment listing, for cross-referencing the commitment_id
-- values embedded in lift_supplemented.poa_commitment).
SELECT *
FROM {{ ref('stg__hpd_rezoning_tracker') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY commitment_id ORDER BY year DESC) = 1
