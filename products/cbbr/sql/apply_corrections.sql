-- Create multi geometry if the records has the same unique_id into a CTE table 
-- and rename the geometry column from wkt to _multipoint_geom to highlight the change in geometry type

-- Create temp update points corrections table

WITH _cbbr_point_corrections AS (
    SELECT
        unique_id,
        ST_MULTI(ST_COLLECT(geom)) AS _geom
    FROM _cbbr_point_corrections
    GROUP BY unique_id
)

-- update _cbbr_submissions table with update geoms from manually mapped corrections table 
UPDATE _cbbr_submissions a
SET
    geom = b._geom,
    geo_function = 'Manual_Research'
FROM _cbbr_point_corrections AS b
WHERE a.unique_id = b.unique_id;


--- Create temp update line corrections table
WITH _cbbr_line_corrections AS (
    SELECT
        unique_id,
        ST_UNION(geom) AS _geom
    FROM _cbbr_line_corrections
    GROUP BY unique_id
)

-- update _cbbr_submissions table with update geoms from manually mapped line corrections table
UPDATE _cbbr_submissions a
SET
    geom = b._geom,
    geo_function = 'Manual_Research'
FROM _cbbr_line_corrections AS b
WHERE a.unique_id = b.unique_id;


--- Create temp update polygon corrections table
WITH _cbbr_poly_corrections AS (
    SELECT
        unique_id,
        ST_UNION(geom) AS _geom
    FROM _cbbr_poly_corrections
    GROUP BY unique_id
)

-- update _cbbr_submissions table with update geoms from manually mapped polygon corrections table
UPDATE _cbbr_submissions a
SET
    geom = b._geom,
    geo_function = 'Manual_Research'
FROM _cbbr_poly_corrections AS b
WHERE a.unique_id = b.unique_id;
