SELECT *
FROM {{ ref('cpdb_dcpattributes') }}
WHERE ST_GEOMETRYTYPE(geom) = 'ST_MultiPoint'
