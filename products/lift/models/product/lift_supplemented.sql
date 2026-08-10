-- Drop-in supplemented version of lift_csv: same columns, same grain (one row
-- per bbl), with the three empty placeholder columns from the source
-- spreadsheet populated:
--   displacement_risk_formula <- DRI tier, via pluto/census-tract -> NTA (int__lift_dri)
--   cpspenttotal / cpprojects <- spatial join to CPDB points (int__lift_cpdb)
-- bbls with no intersecting capital projects get 0, not null, for cpspenttotal/cpprojects.
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
        COALESCE(cpdb.cp_projects, 0) AS cpprojects
    FROM lift
    LEFT JOIN dri ON lift.bbl = dri.bbl
    LEFT JOIN cpdb ON lift.bbl = cpdb.bbl
)

SELECT * FROM final
