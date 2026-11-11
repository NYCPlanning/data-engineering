-- Leaves out OwnerName and Comments, which name private owners and sellers.
SELECT
    "Watershed" AS watershed,
    "Type" AS property_type,
    "Block" AS block,
    "Lot" AS lot,
    ST_Transform(ST_MakeValid(geom), 'EPSG:2263', 'EPSG:4326', true) AS geom
FROM {{ source('sources', 'dcp_bluebelt_properties') }}
