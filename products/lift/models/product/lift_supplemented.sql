-- Supplemented version of lift_csv: same grain (one row per bbl), with the
-- six empty placeholder columns from the source spreadsheet populated:
--   displacement_risk_formula <- DRI tier, via pluto/census-tract -> NTA (int__lift_dri)
--   cpspenttotal / cpprojects <- spatial join to CPDB points (int__lift_cpdb)
--   laa <- spatial join to HPD Limited Affordability Areas (int__lift_laa)
--   poa / poa_commitment <- NYC Rezoning Tracker, via bbl's NTA -> rezoning area
--     (int__lift_rezoning_tracker)
-- bbls with no intersecting capital projects get 0, not null, for cpspenttotal/cpprojects.
-- laa/poa/poa_commitment get NULL (not 'N'/''), per the data dictionary's Y/NULL spec
-- for laa and by analogy for the other two (no matching commitments looks the same as
-- no data, unlike cpspenttotal/cpprojects where 0 is a meaningful count).
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

rezoning AS (
    SELECT * FROM {{ ref('int__lift_rezoning_tracker') }}
),

pluto AS (
    SELECT bbl, trnstzone FROM {{ ref('stg__pluto') }}
),

final AS (
    SELECT
        lift.* EXCLUDE (displacement_risk_formula, cpspenttotal, cpprojects, laa, poa, poa_commitment),
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
        -- e.g. "63 commitments (41 done, 12 in progress, 10 other)" - counts of
        -- rezoning-tracker commitments tied to the bbl's NTA, current status only
        -- (see int__lift_rezoning_tracker for the "current" definition)
        CASE
            WHEN rezoning.n_commitments IS NOT NULL
                THEN rezoning.n_commitments || ' commitments (' || rezoning.n_done || ' done, '
                    || rezoning.n_in_progress || ' in progress, '
                    || (rezoning.n_commitments - rezoning.n_done - rezoning.n_in_progress) || ' other)'
        END AS poa,
        rezoning.commitment_detail AS poa_commitment
    FROM lift
    LEFT JOIN dri ON lift.bbl = dri.bbl
    LEFT JOIN cpdb ON lift.bbl = cpdb.bbl
    LEFT JOIN pluto ON lift.bbl = pluto.bbl
    LEFT JOIN laa ON lift.bbl = laa.bbl
    LEFT JOIN rezoning ON lift.bbl = rezoning.bbl
)

SELECT * FROM final
