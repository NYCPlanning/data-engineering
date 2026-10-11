-- a name version runs over a code's consecutive releases with the same name; releases the
-- code is missing from don't split it, so a new version only starts when the name changes
WITH releases AS (
    SELECT
        project_type,
        typc,
        name,
        release_order,
        row_number() OVER (PARTITION BY project_type, typc ORDER BY release_order)
        - row_number() OVER (
            PARTITION BY project_type, typc, name ORDER BY release_order
        ) AS name_run
    FROM {{ ref('int__ten_year_plan_category_releases') }}
),

runs AS (
    SELECT
        project_type,
        typc,
        name,
        min(release_order) AS first_release_order,
        max(release_order) AS last_release_order
    FROM releases
    GROUP BY project_type, typc, name, name_run
)

SELECT
    runs.project_type,
    runs.typc,
    runs.name,
    first_release.fisa_version AS first_release,
    last_release.fisa_version AS last_release,
    runs.first_release_order,
    runs.last_release_order
FROM runs
INNER JOIN {{ ref('int__fisa_releases') }} AS first_release
    ON runs.first_release_order = first_release.release_order
INNER JOIN {{ ref('int__fisa_releases') }} AS last_release
    ON runs.last_release_order = last_release.release_order
