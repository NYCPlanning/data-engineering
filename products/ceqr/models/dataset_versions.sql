WITH source_data_versions AS (SELECT * FROM {{ source("recipe_sources", "source_data_versions") }}),

data_hub_datasets AS (SELECT * FROM {{ ref("data_hub_datasets") }}),

recipe_versions AS (
    SELECT
        data_hub_datasets.dataset_id,
        data_hub_datasets.dataset_name,
        data_hub_datasets.availability_type,
        data_hub_datasets.file_type,
        data_hub_datasets.geometry_type,
        source_data_versions.v AS version
    FROM data_hub_datasets
    LEFT JOIN source_data_versions ON data_hub_datasets.dataset_id = source_data_versions.schema_name
),

add_non_recipe_versions AS (
    SELECT
        dataset_id,
        dataset_name,
        availability_type,
        file_type,
        geometry_type,
        CASE
            WHEN availability_type != 'download' THEN NULL
            ELSE COALESCE(version, '{{ var('version') }}')
        END AS version
    FROM recipe_versions
)

SELECT * FROM add_non_recipe_versions
