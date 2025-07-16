-- attribute maprojid --> facilities map
DROP TABLE IF EXISTS attributes_maprojid_facilities;
CREATE TABLE attributes_maprojid_facilities AS

WITH master AS (
    WITH singlename AS (
        WITH filtered AS (
            SELECT
                facname,
                count(facname) AS namecount
            FROM dcp_facilities
            GROUP BY facname
        )

        SELECT facname
        FROM filtered
        WHERE namecount = 1
    )

    SELECT
        a.maprojid,
        a.magency,
        a.projectid,
        a.description,
        b.facname,
        b.uid,
        b.wkb_geometry AS geom
    FROM cpdb_dcpattributes AS a, dcp_facilities AS b
    WHERE
        a.geom IS NULL
        AND a.magency IN ('850', '801', '806', '126', '819', '57', '72', '858', '827', '71', '56', '816', '125', '998')
        AND b.facname LIKE '%' || ' ' || '%' || ' ' || '%'
        AND upper(a.description) LIKE '%' || upper(b.facname) || '%'
        AND b.facname IN (SELECT facname FROM singlename)
),

lib_master AS (
    SELECT
        a.maprojid,
        a.magency,
        a.projectid,
        a.description,
        b.facname,
        b.uid,
        b.wkb_geometry AS geom
    FROM cpdb_dcpattributes AS a,
        (
            SELECT * FROM dcp_facilities
            WHERE facgroup = 'Libraries'
        ) AS b
    WHERE
        a.magency::int IN (39, 37, 38, 35)
        AND upper(a.description) NOT LIKE '%AND%'
        AND '%' || upper(a.description) || '%' LIKE '%' || upper(b.facname) || '%'
)

SELECT
    master.maprojid,
    master.uid
FROM master, cpdb_dcpattributes
WHERE
    cpdb_dcpattributes.magency = master.magency
    AND cpdb_dcpattributes.projectid = master.projectid
    AND cpdb_dcpattributes.geom IS NULL
    AND master.geom IS NOT NULL
UNION
SELECT
    lib_master.maprojid,
    lib_master.uid
FROM lib_master, cpdb_dcpattributes
WHERE
    cpdb_dcpattributes.magency = lib_master.magency
    AND cpdb_dcpattributes.projectid = lib_master.projectid
    AND cpdb_dcpattributes.geom IS NULL;
