WITH final AS (
    SELECT
        bbl::decimal::bigint::text,
        bin,
        units_co::numeric,
        classa_prop::numeric,
        job_type,
        job_status,
        date_complete::date
    FROM {{ source("recipe_sources", "dcp_developments") }}
)
SELECT * FROM final
