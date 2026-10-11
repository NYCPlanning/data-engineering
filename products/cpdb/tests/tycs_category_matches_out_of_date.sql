-- hand matches whose code isn't in any FISA release or whose TYCS category isn't in any
-- TYCS release: the row is wrong or no longer needed
{{ config(severity='warn') }}

SELECT m.*
FROM {{ ref('tycs_category_matches') }} AS m
LEFT JOIN {{ ref('int__ten_year_plan_categories') }} AS c
    ON m.project_type = c.project_type AND m.typc = c.typc
LEFT JOIN {{ ref('int__tycs_categories') }} AS t
    ON m.tycs_project_type = t.tycs_project_type AND m.tycs_category = t.tycs_category
WHERE c.typc IS NULL OR t.tycs_category IS NULL
