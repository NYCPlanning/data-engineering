--Update geom to be multi
UPDATE
    _cbbr_submissions
SET
    geom = ST_Multi (geom)
WHERE
    ST_GeometryType (geom) IN ('ST_Polygon', 'ST_Point', 'ST_LineString');

--Update lines to be polygons
UPDATE
    _cbbr_submissions
SET
    geom = ST_Buffer (geom::geography, 15)::geometry
WHERE
    ST_GeometryType (geom) = 'ST_MultiLineString';

--Update geom to be multi
UPDATE
    _cbbr_submissions
SET
    geom = ST_Multi (geom)
WHERE
    ST_GeometryType (geom) IN ('ST_Polygon', 'ST_Point', 'ST_LineString');

