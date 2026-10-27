{{ config(
    materialized = 'table'
) }}

SELECT
    b7sc,
    lookup_key,
    place_name_sort_order,
    face_code,
    feature_type
FROM {{ ref('stg__facecode_and_featurename') }}
WHERE principal_flag = 'Y'
-- A B7SC can have two principal names in one table (a source defect). Prod looks a name up
-- with an unordered query on one table and takes the first row, which is the lowest
-- OBJECTID; ogc_fid follows OBJECTID order (GDAL reads OpenFileGDB layers in FID order).
QUALIFY
    b7sc IS NULL
    OR ROW_NUMBER() OVER (PARTITION BY b7sc, feature_type ORDER BY ogc_fid) = 1
