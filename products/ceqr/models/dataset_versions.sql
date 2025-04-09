WITH source_data_versions AS (SELECT * FROM {{ source("recipe_sources", "source_data_versions") }}),

datasets AS (SELECT * FROM {{ ref("datasets") }}),

-- preserve the order of rows in the seed file
datasets_indexed AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY (SELECT NULL)
        ) AS row_number,
        *
    FROM datasets
),

recipe_versions AS (
    SELECT
        datasets_indexed.dataset_name,
        datasets_indexed.dataset_id,
        (CASE
            WHEN datasets_indexed.availability_type = 'webpage'
                THEN NULL
            ELSE COALESCE(source_data_versions.v, '{{ var('build_version') }}')
        END) AS version,
        datasets_indexed.availability_type,
        datasets_indexed.file_type,
        datasets_indexed.geometry_type,
        datasets_indexed.source_url
    FROM datasets_indexed
    LEFT JOIN source_data_versions ON datasets_indexed.dataset_id = source_data_versions.schema_name
    ORDER BY row_number ASC
)

SELECT * FROM recipe_versions
