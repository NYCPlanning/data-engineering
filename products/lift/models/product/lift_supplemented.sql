-- Supplemented version of lift_csv: same grain (one row per bbl), with the
-- empty placeholder columns from the source spreadsheet populated:
--   displacement_risk_formula <- DRI tier, via pluto/census-tract -> NTA (int__lift_dri)
--   cpspenttotal / cpprojects <- spatial join to CPDB points (int__lift_cpdb)
--   laa <- spatial join to HPD Limited Affordability Areas (int__lift_laa)
--   poa / poa_commitment <- HPD's Points of Agreement (POA) commitments tracker, by
--     bbl directly (stg__hpd_poa_commitments) - poa is the tracker's site name, and
--     poa_commitment is its free-text commitment narrative; naive best-effort mapping
--     (the source has no separate short/long fields), not yet confirmed with HPD -
--     see README for open questions.
--   ahft <- 'Y' if the bbl's community district is in the bottom 12 CDs by rate of
--     affordable housing development (DCP's AHFT tracker, stg__dcp_housing_ahft),
--     else NULL. Joined by community district (lift.cd <-> stg__dcp_housing_ahft's
--     derived borocd), not bbl - see README for the CD key derivation.
--   zzz_lift_data_zzz, unitpotential, dev_flags, redev_summary, redev_strategy,
--     redev_priority, redev_study, cp_remediation, siteprep_costs, siteprep_desc,
--     rlv_amount, rlv_desc <- City Hall's "Public Sites for Housing" tracker, by bbl
--     (stg__public_sites_for_housing). Field mapping supplied by City Hall via email
--     (2026-09-21); naive best-effort mapping, not yet confirmed with them - see
--     README for open questions (redev_priority's source field doesn't exist verbatim,
--     and zzz_lift_data_zzz is documented elsewhere as a section-boundary marker, not
--     a data field, so mapping "Site" into it is unusual). capitalneeds_unfunded was
--     explicitly excluded from the mapping ("Strike, not needed") and is left untouched.
-- bbls with no intersecting capital projects get 0, not null, for cpspenttotal/cpprojects.
-- laa/poa/poa_commitment/ahft get NULL (not 'N'/''), per the data dictionary's Y/NULL
-- spec for laa and ahft and by analogy for poa/poa_commitment (no matching commitments
-- looks the same as no data, unlike cpspenttotal/cpprojects where 0 is a meaningful
-- count). The new Public Sites fields get NULL for bbls with no matching row there
-- (source has no Y/NULL convention of its own to follow).
-- Five new columns (not source placeholders):
--   boroct2020 / nta2020 / ntaname - census tract & NTA a bbl resolves to (int__lift_dri).
--     Populated independently of displacement_risk_formula - a bbl in a park/cemetery/airport
--     NTA (no DRI value, see README) still gets its real census tract/NTA here.
--   cp_project_ids / cp_project_descriptions - comma/pipe-delimited lists of the intersecting
--     CPDB project ids and descriptions, for QA/traceability behind cpspenttotal/cpprojects.
--     Both are kept as JSON arrays in int__lift_cpdb (nicer to work with in SQL/QA queries) and
--     flattened here since a raw JSON array reads awkwardly (`["a","b"]`) in an exported CSV
--     cell. Different delimiters because project ids are safe alphanumeric codes (no internal
--     commas) but free-text descriptions sometimes contain literal commas (e.g. "BROOKLYN BR
--     (#6)  BK APPR'S, MAIN SPAN & PAINT..."), which would make a comma-split ambiguous.
--     cp_project_ids[i] and cp_project_descriptions[i] refer to the same project (both ordered
--     by project_id in int__lift_cpdb). '' (not null) for bbls with no intersecting projects,
--     matching the 0-not-null convention on cpspenttotal/cpprojects.
--   trnstzone - PLUTO's transit zone designation, carried straight through from dcp_mappluto_wi
--     via stg__pluto.
WITH lift AS (
    SELECT * FROM {{ ref('stg__lift_csv') }}
),

dri AS (
    SELECT * FROM {{ ref('int__lift_dri') }}
),

cpdb AS (
    SELECT * FROM {{ ref('int__lift_cpdb') }}
),

laa AS (
    SELECT * FROM {{ ref('int__lift_laa') }}
),

poa_commitments AS (
    SELECT * FROM {{ ref('stg__hpd_poa_commitments') }}
),

ahft AS (
    SELECT * FROM {{ ref('stg__dcp_housing_ahft') }}
),

pluto AS (
    SELECT bbl, trnstzone FROM {{ ref('stg__pluto') }}
),

public_sites AS (
    SELECT * FROM {{ ref('stg__public_sites_for_housing') }}
),

final AS (
    SELECT
        lift.* EXCLUDE (
            displacement_risk_formula, cpspenttotal, cpprojects, laa, poa, poa_commitment, ahft,
            zzz_lift_data_zzz, unitpotential, dev_flags, redev_summary, redev_strategy,
            redev_priority, redev_study, cp_remediation, siteprep_costs, siteprep_desc,
            rlv_amount, rlv_desc
        ),
        dri.dri_tier AS displacement_risk_formula,
        dri.boroct2020,
        dri.nta2020,
        dri.ntaname,
        COALESCE(cpdb.cp_spent_total, 0) AS cpspenttotal,
        COALESCE(cpdb.cp_projects, 0) AS cpprojects,
        ARRAY_TO_STRING(CAST(COALESCE(cpdb.cp_project_ids, '[]') AS VARCHAR []), ',') AS cp_project_ids,
        ARRAY_TO_STRING(
            CAST(COALESCE(cpdb.cp_project_descriptions, '[]') AS VARCHAR []), ' | '
        ) AS cp_project_descriptions,
        pluto.trnstzone,
        -- 'Y'/NULL, not 'Y'/'N' - matches the data dictionary's LAA spec directly
        CASE WHEN laa.bbl IS NOT NULL THEN 'Y' END AS laa,
        poa_commitments.site AS poa,
        poa_commitments.poa_community AS poa_commitment,
        -- bottom 12 of 59 CDs by rate of affordable housing development (ahft_rank 1-12)
        CASE WHEN ahft.ahft_rank <= 12 THEN 'Y' END AS ahft,
        public_sites.site AS zzz_lift_data_zzz,
        public_sites.unitpot AS unitpotential,
        public_sites.key_challenges AS dev_flags,
        public_sites.summary AS redev_summary,
        public_sites.redevelopment_strategy AS redev_strategy,
        public_sites.prioritization AS redev_priority,
        public_sites.study_summary AS redev_study,
        public_sites.omb_remediation_cost AS cp_remediation,
        public_sites.site_preparation_costs AS siteprep_costs,
        public_sites.site_preparation_description AS siteprep_desc,
        public_sites.rlv AS rlv_amount,
        public_sites.rlv_desc AS rlv_desc
    FROM lift
    LEFT JOIN dri ON lift.bbl = dri.bbl
    LEFT JOIN cpdb ON lift.bbl = cpdb.bbl
    LEFT JOIN pluto ON lift.bbl = pluto.bbl
    LEFT JOIN laa ON lift.bbl = laa.bbl
    LEFT JOIN poa_commitments ON lift.bbl = poa_commitments.bbl
    LEFT JOIN ahft ON lift.cd = ahft.borocd
    LEFT JOIN public_sites ON lift.bbl = public_sites.bbl
)

SELECT * FROM final
