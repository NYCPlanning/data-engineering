-- City Hall's "Public Sites for Housing" tracker, joined onto LIFT by bbl (see
-- lift_supplemented.sql - this is the crosswalk DCP asked us to add). bbl here is
-- source-provided and not guaranteed clean: ~4% of rows have no bbl, a handful list
-- several bbls in one cell (a site spanning multiple tax lots - not split out here,
-- see README) or a non-numeric value, and a few bbls appear on more than one row
-- (distinct named sub-sites/proposals sharing one tax lot, e.g. two competing
-- redevelopment concepts for the same building). TRY_CAST drops the unparseable
-- rows; QUALIFY keeps one row per bbl (first in source order) so this stays
-- joinable 1:1 against lift_supplemented's bbl grain - the dropped/deduped rows are
-- a known gap to raise with City Hall.
WITH raw AS (
    SELECT * FROM {{ source('recipe_sources', 'public_sites_for_housing') }}
),

final AS (
    SELECT
        TRY_CAST(bbl AS BIGINT) AS bbl,
        -- Hand-typed text with stray leading/trailing spaces (e.g. ' $384,202 ').
        TRIM(site) AS site,
        TRIM(unitpot) AS unitpot,
        TRIM(key_challenges) AS key_challenges,
        TRIM(summary) AS summary,
        TRIM(redevelopment_strategy) AS redevelopment_strategy,
        TRIM(prioritization) AS prioritization,
        TRIM(study_summary) AS study_summary,
        TRIM(omb_remediation_cost) AS omb_remediation_cost,
        TRIM(site_preparation_costs) AS site_preparation_costs,
        TRIM(site_preparation_description) AS site_preparation_description,
        TRIM(rlv) AS rlv,
        TRIM(rlv_desc) AS rlv_desc
    FROM raw
    WHERE TRY_CAST(bbl AS BIGINT) IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRY_CAST(bbl AS BIGINT) ORDER BY ogc_fid) = 1
)

SELECT * FROM final
