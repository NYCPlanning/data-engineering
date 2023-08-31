-- calculate how much (total area and percentage) of each lot is covered by a special purpose district
-- assign the special purpose district(s) to each tax lot
-- the order special purpose districts are assigned is based on which district covers the majority of the lot
-- a district is only assigned if more than 10% of the lot is covered by the district
-- OR if no lot meets the 10% threshold, than the lot with the highest percentage of the special district is assigned it
DROP TABLE IF EXISTS specialpurposeper;
CREATE TABLE specialpurposeper AS
SELECT
    p.bbl,
    n.sdlbl,
    ST_AREA(
        CASE
            WHEN ST_COVEREDBY(p.geom, n.geom)
                THEN p.geom
            ELSE
                ST_MULTI(ST_INTERSECTION(p.geom, n.geom))
        END
    ) AS segbblgeom,
    ST_AREA(p.geom) AS allbblgeom,
    ST_AREA(
        CASE
            WHEN ST_COVEREDBY(n.geom, p.geom)
                THEN n.geom
            ELSE
                ST_MULTI(ST_INTERSECTION(n.geom, p.geom))
        END
    ) AS segzonegeom,
    ST_AREA(n.geom) AS allzonegeom
FROM dof_dtm AS p
INNER JOIN dcp_specialpurpose AS n
    ON ST_INTERSECTS(p.geom, n.geom);

DROP TABLE IF EXISTS specialpurposeperorder;
CREATE TABLE specialpurposeperorder AS
WITH specialpurposeperorder_init AS (
    SELECT
        bbl,
        sdlbl,
        segbblgeom,
        (segbblgeom / allbblgeom) * 100 AS perbblgeom,
        (segzonegeom / allzonegeom) * 100 AS perzonegeom,
        -- per sp district type, rank by 
        --   1) if lot meets 10% coverage by sp district threshold
        --   2) area of coverage
        -- This is to get cases where special sp district may only be assigned to one lot. 
        -- 1) is to cover edge case where largest section of specific large (but common) sp district that happens to have largest area on small fraction of huge lot
        -- See BBL 1013730001 in ZoLa - largest section of special district but should not be assigned spdist SRI
        ROW_NUMBER()
            OVER (PARTITION BY sdlbl ORDER BY (segbblgeom / allbblgeom) * 100 < 10, segzonegeom DESC)
        AS sd_row_number
    FROM specialpurposeper
)

SELECT
    bbl,
    sdlbl,
    segbblgeom,
    ROW_NUMBER() OVER (PARTITION BY bbl ORDER BY segbblgeom ASC, sdlbl DESC) AS row_number
FROM specialpurposeperorder_init
WHERE
    perbblgeom >= 10
    OR sd_row_number = 1;

UPDATE dcp_zoning_taxlot a
SET specialdistrict1 = sdlbl
FROM specialpurposeperorder AS b
WHERE
    a.bbl = b.bbl
    AND row_number = 1;

UPDATE dcp_zoning_taxlot a
SET specialdistrict2 = sdlbl
FROM specialpurposeperorder AS b
WHERE
    a.bbl = b.bbl
    AND row_number = 2;

UPDATE dcp_zoning_taxlot a
SET specialdistrict3 = sdlbl
FROM specialpurposeperorder AS b
WHERE
    a.bbl = b.bbl
    AND row_number = 3;

-- set the order of special districts 
UPDATE dcp_zoning_taxlot
SET
    specialdistrict1 = 'CL',
    specialdistrict2 = 'MiD'
WHERE
    specialdistrict1 = 'MiD'
    AND specialdistrict2 = 'CL';
UPDATE dcp_zoning_taxlot
SET
    specialdistrict1 = 'MiD',
    specialdistrict2 = 'TA'
WHERE
    specialdistrict1 = 'TA'
    AND specialdistrict2 = 'MiD';
UPDATE dcp_zoning_taxlot
SET
    specialdistrict1 = '125th',
    specialdistrict2 = 'TA'
WHERE
    specialdistrict1 = 'TA'
    AND specialdistrict2 = '125th';
UPDATE dcp_zoning_taxlot
SET
    specialdistrict1 = 'EC-5',
    specialdistrict2 = 'MX-16'
WHERE
    specialdistrict1 = 'MX-16'
    AND specialdistrict2 = 'EC-5';
UPDATE dcp_zoning_taxlot
SET
    specialdistrict1 = 'EC-6',
    specialdistrict2 = 'MX-16'
WHERE
    specialdistrict1 = 'MX-16'
    AND specialdistrict2 = 'EC-6';
UPDATE dcp_zoning_taxlot
SET
    specialdistrict1 = 'EHC',
    specialdistrict2 = 'TA'
WHERE
    specialdistrict1 = 'TA'
    AND specialdistrict2 = 'EHC';
UPDATE dcp_zoning_taxlot
SET
    specialdistrict1 = 'MX-1',
    specialdistrict2 = 'G'
WHERE
    specialdistrict1 = 'G'
    AND specialdistrict2 = 'MX-1';
