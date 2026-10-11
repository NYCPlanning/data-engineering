-- categories in the latest TYCS release that no code matches, by name or by hand. A new
-- one usually means the strategy renamed a category: add it to tycs_category_matches.
-- Reads only intermediate models so it runs with them, not in cpdb.sh's staging step.
{{ config(severity='warn') }}

SELECT t.*
FROM {{ ref('int__tycs_categories') }} AS t
LEFT JOIN {{ ref('int__ten_year_plan_category_tycs_matches') }} AS m
    ON t.tycs_project_type = m.tycs_project_type AND t.tycs_category = m.tycs_category
WHERE
    t.last_pub_date = (SELECT max(last_pub_date) FROM {{ ref('int__tycs_categories') }})
    AND m.typc IS NULL
