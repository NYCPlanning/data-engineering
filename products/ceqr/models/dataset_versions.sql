WITH source_data_versions AS (SELECT * FROM {{ source("recipe_sources", "source_data_versions") }}),

data_hub_datasets AS (SELECT * FROM {{ ref("data_hub_datasets") }}),

recipe_versions AS (
    SELECT
        data_hub_datasets.dataset_id,
        data_hub_datasets.dataset_name,
        data_hub_datasets.availability_type,
        data_hub_datasets.file_type,
        data_hub_datasets.geometry_type,
        COALESCE(source_data_versions.v, '{{ var('build_version') }}') AS version
    FROM data_hub_datasets
    LEFT JOIN source_data_versions ON data_hub_datasets.dataset_id = source_data_versions.schema_name
)

SELECT * FROM recipe_versions
