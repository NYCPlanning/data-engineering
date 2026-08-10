WITH cpdb_raw AS (
    SELECT * FROM {{ source('recipe_sources', 'cpdb_projects_points') }}
),

final AS (
    SELECT
        maprojid AS project_id,
        descript AS description,
        -- "Sum of the total funding spent associated with the project within the
        -- City's budget" per product-metadata/products/cpdb/projects/metadata.yml
        -- (id: spent_total). sptotalcb (spent_total_checkbooknyc) is a separate,
        -- Checkbook-NYC-sourced total - not used here.
        sptotal AS spent_total,
        geom
    FROM cpdb_raw
)

SELECT * FROM final
