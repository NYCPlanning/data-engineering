-- Supplemented version of lift_csv: same grain (one row per bbl), with the
-- three empty placeholder columns from the source spreadsheet populated:
--   displacement_risk_formula <- DRI tier, via pluto/census-tract -> NTA (int__lift_dri)
--   cpspenttotal / cpprojects <- spatial join to CPDB points (int__lift_cpdb)
-- bbls with no intersecting capital projects get 0, not null, for cpspenttotal/cpprojects.
-- cp_project_ids is a new column (not a source placeholder) - a comma-delimited list of the
-- intersecting CPDB project ids, for QA/traceability behind cpspenttotal/cpprojects. Kept as a
-- JSON array in int__lift_cpdb (nicer to work with in SQL/QA queries); flattened to
-- comma-delimited here since a raw JSON array reads awkwardly (`["a","b"]`) in an exported CSV
-- cell. '' (not null) for bbls with no intersecting projects, matching the 0-not-null convention
-- on cpspenttotal/cpprojects.
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
        COALESCE(cpdb.cp_spent_total, 0) AS cpspenttotal,
        COALESCE(cpdb.cp_projects, 0) AS cpprojects,
        ARRAY_TO_STRING(CAST(COALESCE(cpdb.cp_project_ids, '[]') AS VARCHAR []), ',') AS cp_project_ids
    FROM lift
    LEFT JOIN dri ON lift.bbl = dri.bbl
    LEFT JOIN cpdb ON lift.bbl = cpdb.bbl
)

SELECT * FROM final
