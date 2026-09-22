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
        site,
        unitpot,
        key_challenges,
        summary,
        redevelopment_strategy,
        prioritization,
        study_summary,
        omb_remediation_cost,
        site_preparation_costs,
        site_preparation_description,
        rlv,
        rlv_desc
    FROM raw
    WHERE TRY_CAST(bbl AS BIGINT) IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRY_CAST(bbl AS BIGINT) ORDER BY ogc_fid) = 1
)

SELECT * FROM final
