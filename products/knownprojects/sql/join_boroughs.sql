SELECT
    a.*,
    b.borocode::VARCHAR AS borough
INTO _combined
FROM combined AS a
LEFT JOIN dcp_boroboundaries AS b ON ST_INTERSECTS(a.geom, b.wkb_geometry);

DROP TABLE combined;
SELECT * INTO combined FROM _combined;
DROP TABLE _combined;

WITH straddling_records AS (
    SELECT record_id
    FROM combined
    GROUP BY record_id
    HAVING COUNT(borough) > 1
),

max_area AS (
    SELECT DISTINCT ON (combined.record_id)
        combined.record_id,
        combined.borough
    FROM straddling_records AS s
    INNER JOIN combined ON s.record_id = combined.record_id
    INNER JOIN dcp_boroboundaries AS b ON b.borocode::VARCHAR = combined.borough
    ORDER BY
        combined.record_id,
        ST_AREA(ST_INTERSECTION(combined.geom, b.wkb_geometry))
        / ST_AREA(combined.geom) DESC
)

DELETE FROM combined
USING max_area
WHERE
    combined.record_id = max_area.record_id
    AND combined.borough != max_area.borough;
