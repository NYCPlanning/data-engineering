-- Supplemented version of lift_csv: same grain (one row per bbl), with the
-- three empty placeholder columns from the source spreadsheet populated:
--   displacement_risk_formula <- DRI tier, via pluto/census-tract -> NTA (int__lift_dri)
--   cpspenttotal / cpprojects <- spatial join to CPDB points (int__lift_cpdb)
-- bbls with no intersecting capital projects get 0, not null, for cpspenttotal/cpprojects.
-- Four new columns (not source placeholders):
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
WITH lift AS (
    SELECT * FROM {{ ref('stg__lift_csv') }}
),

dri AS (
    SELECT * FROM {{ ref('int__lift_dri') }}
),

cpdb AS (
    SELECT * FROM {{ ref('int__lift_cpdb') }}
),

final AS (
    SELECT
        lift.* EXCLUDE (displacement_risk_formula, cpspenttotal, cpprojects),
        dri.dri_tier AS displacement_risk_formula,
        dri.boroct2020,
        dri.nta2020,
        dri.ntaname,
        COALESCE(cpdb.cp_spent_total, 0) AS cpspenttotal,
        COALESCE(cpdb.cp_projects, 0) AS cpprojects,
        ARRAY_TO_STRING(CAST(COALESCE(cpdb.cp_project_ids, '[]') AS VARCHAR []), ',') AS cp_project_ids,
        ARRAY_TO_STRING(
            CAST(COALESCE(cpdb.cp_project_descriptions, '[]') AS VARCHAR []), ' | '
        ) AS cp_project_descriptions
    FROM lift
    LEFT JOIN dri ON lift.bbl = dri.bbl
    LEFT JOIN cpdb ON lift.bbl = cpdb.bbl
)

SELECT * FROM final
