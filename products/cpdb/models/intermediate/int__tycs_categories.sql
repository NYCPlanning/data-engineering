-- each TYCS category, with the releases that published it and its program's description
-- in the latest of them
WITH releases AS (
    SELECT
        projtype AS tycs_project_type,
        typc AS tycs_category,
        projtype_desc,
        pub_date,
        row_number() OVER (PARTITION BY projtype, typc ORDER BY pub_date DESC) AS recency
    FROM {{ ref('stg__omb_tycs') }}
)

SELECT
    tycs_project_type,
    tycs_category,
    min(pub_date) AS first_pub_date,
    max(pub_date) AS last_pub_date,
    max(projtype_desc) FILTER (WHERE recency = 1) AS tycs_project_type_desc
FROM releases
GROUP BY tycs_project_type, tycs_category
