{{ config(materialized='table') }}

SELECT
    schema_name AS dataset_id,
    dataset_name,
    v AS version,
    file_type,
    archive_date,
    url
FROM {{ source('recipe_sources', 'source_data_versions') }}
