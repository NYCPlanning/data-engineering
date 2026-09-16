-- Full commitment listing (current status only, one row per commitment_id), exported
-- so commitment_id values embedded in lift_supplemented.poa_commitment can be traced
-- back to their full detail. commitment_id is artificial - see
-- stg__hpd_rezoning_tracker - stable within a build, not a source-provided key.
SELECT
    commitment_id,
    rezoning_area,
    commitment_title,
    rezoning_policy_domain,
    lead_agency,
    commitment_stage,
    year,
    original_schedule,
    statement_from_source_document_poa,
    narrative
FROM {{ ref('int__rezoning_commitments_latest') }}
ORDER BY commitment_id
