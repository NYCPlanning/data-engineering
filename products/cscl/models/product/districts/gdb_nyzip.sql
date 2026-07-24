{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

SELECT
    d.po_name AS "PO_NAME",
    d.county AS "COUNTY",
    d.created_by AS "CREATED_BY",
    d.created_date AS "CREATED_DATE",
    d.modified_by AS "MODIFIED_BY",
    d.modified_date AS "MODIFIED_DATE",
    d.zip_code AS "ZIP_CODE",
    d.geom,
    st_perimeter(d.geom) AS "SHAPE_Length",
    st_area(d.geom) AS "SHAPE_Area"
FROM {{ ref('stg__zipcode') }} AS d
