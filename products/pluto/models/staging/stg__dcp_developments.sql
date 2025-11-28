WITH final AS (
    SELECT
        bbl,
        bin,
        units_co,
        classa_prop,
        job_type,
        job_status,
        date_complete
    FROM {{ source("recipe_sources", "dcp_developments") }}
)
SELECT * FROM final
