-- get a list of records that go not fall w/ in the expected community board
COPY (
    SELECT
        a.regid,
        a.sitename,
        a.description,
        a.commdist,
        b.borocd,
        a.geomsource
    FROM cbbr_submissions AS a
    LEFT JOIN dcp_cdboundaries AS b
        ON ST_WITHIN(a.geom, b.geom)
    WHERE
        a.commdist::text != b.borocd::text
        AND ST_GEOMETRYTYPE(a.geom) = 'ST_MultiPoint'
        AND ST_ISVALID(b.geom)
        AND a.commdist IS NOT NULL
        AND b.borocd IS NOT NULL
) TO '/prod/db-cbbr/analysis/output/cbbr_projects_outsideexpectedcd.csv' DELIMITER ',' CSV HEADER;
