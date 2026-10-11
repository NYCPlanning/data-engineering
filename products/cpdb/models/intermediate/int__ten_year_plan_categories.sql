WITH spans AS (
    SELECT
        project_type,
        typc,
        min(first_release_order) AS first_release_order,
        max(last_release_order) AS last_release_order
    FROM {{ ref('int__ten_year_plan_category_names') }}
    GROUP BY project_type, typc
)

SELECT
    spans.project_type,
    project_types.projecttype AS project_type_name,
    project_types.agencyacronym AS agency,
    spans.typc,
    latest.name,
    first_release.fisa_version AS first_release,
    latest.last_release,
    spans.first_release_order,
    spans.last_release_order
FROM spans
INNER JOIN {{ ref('int__ten_year_plan_category_names') }} AS latest
    ON
        spans.project_type = latest.project_type
        AND spans.typc = latest.typc
        AND spans.last_release_order = latest.last_release_order
INNER JOIN {{ ref('int__fisa_releases') }} AS first_release
    ON spans.first_release_order = first_release.release_order
LEFT JOIN {{ ref('project_types') }} AS project_types
    ON spans.project_type = trim(project_types.projecttypeabbrev)
