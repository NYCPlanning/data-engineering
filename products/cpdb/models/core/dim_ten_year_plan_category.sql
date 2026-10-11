SELECT
    c.project_type,
    c.project_type_name,
    c.agency,
    c.typc,
    c.name,
    c.first_release,
    c.last_release,
    c.first_release_order,
    c.last_release_order,
    s.successor_typc,
    t.tycs_project_type,
    t.tycs_project_type_desc,
    t.tycs_category,
    t.tycs_match,
    t.tycs_last_pub_date,
    l.lifecycle,
    l.lifecycle_confidence
FROM {{ ref('int__ten_year_plan_categories') }} AS c
LEFT JOIN {{ ref('ten_year_plan_category_successors') }} AS s
    ON c.project_type = s.project_type AND c.typc = s.typc
LEFT JOIN {{ ref('int__ten_year_plan_category_tycs_matches') }} AS t
    ON c.project_type = t.project_type AND c.typc = t.typc
LEFT JOIN {{ ref('tycs_category_lifecycles') }} AS l
    ON t.tycs_project_type = l.tycs_project_type AND t.tycs_category = l.tycs_category
