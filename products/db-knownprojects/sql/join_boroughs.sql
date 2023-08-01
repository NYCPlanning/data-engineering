SELECT
    a.*,
    b.borocode::VARCHAR AS borough
INTO _combined
FROM combined a 
LEFT JOIN dcp_boroboundaries b ON ST_intersects(a.geom, b.wkb_geometry);

DROP TABLE combined;
SELECT * INTO combined FROM _combined;
DROP TABLE _combined;

WITH straddling_records AS (
    SELECT record_id
    FROM combined 
    GROUP BY record_id HAVING COUNT(borough) > 1
),
max_area AS (
    SELECT DISTINCT ON (combined.record_id)
        combined.record_id, 
        combined.borough
    FROM straddling_records s
    INNER JOIN combined ON combined.record_id = s.record_id
    INNER JOIN dcp_boroboundaries b ON b.borocode::varchar = combined.borough
    ORDER BY combined.record_id, st_area(st_intersection(combined.geom, b.wkb_geometry))/st_area(combined.geom) DESC
)
DELETE FROM combined 
USING max_area
WHERE combined.record_id = max_area.record_id AND combined.borough != max_area.borough;