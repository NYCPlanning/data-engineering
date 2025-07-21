-- attribute maprojid --> bbl map
DROP TABLE IF EXISTS attributes_maprojid_bbl;
-- Add bbls from ddc
-- then spatial join

-- create temporary table which may have duplicates
CREATE TABLE attributes_maprojid_bbl_tmp AS (
    SELECT *
    FROM (
        SELECT
            a.maprojid,
            b.bbl::text
        FROM cpdb_dcpattributes AS a,
            dcp_facilities AS b
        WHERE
            a.magency::int IN (39, 37, 38, 35)
            AND b.facgroup = 'Libraries'
            AND upper(a.description) NOT LIKE '%AND%'
            AND '%' || upper(a.description) || '%' LIKE '%' || upper(b.facname) || '%'
        UNION
        SELECT
            a.maprojid,
            b.bbl::text
        FROM cpdb_dcpattributes AS a,
            dcp_facilities AS b
        WHERE
            a.magency IN ('850', '801', '806', '126', '819', '57', '72', '858', '827', '71', '56', '816', '125', '998')
            AND b.facname LIKE '%' || ' ' || '%' || ' ' || '%'
            AND upper(a.description) LIKE '%' || upper(b.facname) || '%'
            AND b.facname IN (
                SELECT facname
                FROM (SELECT
                    facname,
                    count(facname) AS namecount
                FROM dcp_facilities
                GROUP BY facname) AS z
                WHERE z.namecount = 1
            )
    ) AS a
    ORDER BY maprojid
);

INSERT INTO attributes_maprojid_bbl_tmp
SELECT
    a.maprojid,
    b.mappluto_bbl
FROM cpdb_dcpattributes AS a,
    doitt_buildingfootprints AS b
WHERE
    st_within(a.geom, b.wkb_geometry)
    AND b.bin IS NOT NULL;


-- create the table dropping duplicates
CREATE TABLE attributes_maprojid_bbl AS (
    SELECT DISTINCT *
    FROM attributes_maprojid_bbl_tmp
);

DROP TABLE attributes_maprojid_bbl_tmp;
