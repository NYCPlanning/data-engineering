WITH rezoning_raw AS (
    SELECT * FROM {{ source('recipe_sources', 'hpd_rezoning_tracker') }}
),

final AS (
    SELECT
        -- Source has no stable id: ogc_fid is just our own CSV row-number, and map_order
        -- (the closest thing to one) isn't reliable - it collides across different
        -- commitment_titles in places, and drifts across years for the same title. This
        -- is an artificial key instead: one id per distinct (rezoning_area,
        -- commitment_title) pair, shared across that commitment's rows in different years
        -- (this dataset is a yearly progress snapshot, so the same commitment recurs).
        -- Stable as long as the set of distinct pairs doesn't change between runs.
        DENSE_RANK() OVER (ORDER BY rezoning_area, commitment_title) AS commitment_id,
        year,
        rezoning_area,
        commitment_title,
        rezoning_policy_domain,
        lead_agency,
        commitment_stage,
        "statement_from_source_document/poa" AS statement_from_source_document_poa,
        original_schedule,
        narrative
    FROM rezoning_raw
)

SELECT * FROM final
