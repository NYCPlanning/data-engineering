-- create staging versions of source data
WITH source AS (
    SELECT * FROM {{ source('dcp_de__data_library', 'nypl_libraries') }}
),

final AS (
    SELECT
        name AS library_name,
        locality AS borough,
        wkb_geometry
    FROM
        source
)

SELECT * FROM final
