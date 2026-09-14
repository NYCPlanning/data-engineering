-- The CPP census-side windows and column-name years come from the
-- cpp_latest_complete_year var, which has to track the dcp_housing pin in
-- recipe.yml. Without this the model keeps reporting a stale decade after a
-- version bump: the numbers stay correct for their labels, so the identity
-- tests still pass and nothing else notices.
--
-- "Latest complete year" is the newest year with a Q4 completion, not
-- max(complete_year). A version cuts mid-year and stragglers land past the
-- cut, so 26Q2 carries a handful of 2026Q3 records while 2026 is nowhere near
-- done.

WITH latest_in_source AS (
    SELECT max(complete_year::numeric) AS year
    FROM {{ source('recipe_sources', 'dcp_housing') }}
    WHERE complete_qrtr LIKE '%Q4'
)

SELECT
    {{ var('cpp_latest_complete_year') }} AS var_cpp_latest_complete_year,
    year AS latest_complete_year_in_dcp_housing
FROM latest_in_source
WHERE year != {{ var('cpp_latest_complete_year') }}
