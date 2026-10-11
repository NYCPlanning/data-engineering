-- a code matches the TYCS category with any name the code has had in FISA, under the same
-- program (the TYCS combines BR and HB). Hand matches in the seed take priority, and when
-- several TYCS categories match, the most recently published one wins.
WITH code_names AS (
    SELECT DISTINCT
        project_type,
        typc,
        upper(name) AS name,
        CASE WHEN project_type IN ('BR', 'HB') THEN 'BR and HB' ELSE project_type END
            AS tycs_project_type
    FROM {{ ref('int__ten_year_plan_category_releases') }}
),

name_matches AS (
    SELECT
        c.project_type,
        c.typc,
        t.tycs_project_type,
        t.tycs_category,
        row_number() OVER (
            PARTITION BY c.project_type, c.typc
            ORDER BY t.last_pub_date DESC, t.tycs_category ASC
        ) AS preference
    FROM code_names AS c
    INNER JOIN {{ ref('int__tycs_categories') }} AS t
        ON c.tycs_project_type = t.tycs_project_type AND c.name = upper(t.tycs_category)
),

matches AS (
    SELECT
        project_type,
        typc,
        tycs_project_type,
        tycs_category,
        'manual' AS tycs_match
    FROM {{ ref('tycs_category_matches') }}
    UNION ALL
    SELECT
        n.project_type,
        n.typc,
        n.tycs_project_type,
        n.tycs_category,
        'name' AS tycs_match
    FROM name_matches AS n
    LEFT JOIN {{ ref('tycs_category_matches') }} AS m
        ON n.project_type = m.project_type AND n.typc = m.typc
    WHERE n.preference = 1 AND m.typc IS NULL
)

SELECT
    m.project_type,
    m.typc,
    m.tycs_project_type,
    t.tycs_project_type_desc,
    m.tycs_category,
    m.tycs_match,
    t.last_pub_date AS tycs_last_pub_date
FROM matches AS m
LEFT JOIN {{ ref('int__tycs_categories') }} AS t
    ON m.tycs_project_type = t.tycs_project_type AND m.tycs_category = t.tycs_category
