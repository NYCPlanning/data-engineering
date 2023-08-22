-- calculate how much (total area and percentage) of each lot is covered by a zoning district
-- assign the zoning district(s) to each tax lot
-- the order zoning districts are assigned is based on which district covers the majority of the lot
-- a district is only assigned if more than 10% of the district covers the lot
-- OR the majority of the district is within the lot
-- split and process by borough to improve processing time
-- join each of the boro tables into one table
-- update each of zoning district fields
-- only say that a lot is within a zoning district if
-- more than 10% of the lot is coverd by the zoning district
-- or more than a specified area is covered by the district
WITH new_order AS (
    SELECT
        bbl,
        zonedist,
        ROW_NUMBER()
            OVER (PARTITION BY bbl ORDER BY priority ASC)
        AS row_number
    FROM (
        SELECT * FROM lotzoneperorder
        WHERE bbl IN (SELECT bbl FROM (
            SELECT
                bbl,
                MAX(segbblgeom) - MIN(segbblgeom) AS diff
            FROM lotzoneperorder
            WHERE perbblgeom >= 10
            GROUP BY bbl
        ) AS a WHERE diff > 0 AND diff < 0.01)
    ) AS a
    INNER JOIN zonedist_priority
        ON a.zonedist = zonedist_priority.zonedist
)

UPDATE lotzoneperorder
SET row_number = new_order.row_number
FROM new_order
WHERE
    lotzoneperorder.bbl = new_order.bbl
    AND lotzoneperorder.zonedist = new_order.zonedist;

UPDATE dcp_zoning_taxlot a
SET zoningdistrict1 = zonedist
FROM lotzoneperorder AS b
WHERE
    a.bbl = b.bbl
    AND row_number = 1
    AND ROUND(perbblgeom::numeric, 2) >= 10;

-- if the largest zoning district is under 10% of entire lot 
-- (e.g. water front lots) 
-- then assign the largest zoning district to be zonedist1
UPDATE dcp_zoning_taxlot a
SET zoningdistrict1 = zonedist
FROM lotzoneperorder AS b
WHERE
    a.bbl = b.bbl
    AND a.zoningdistrict1 IS NULL
    AND row_number = 1;

UPDATE dcp_zoning_taxlot a
SET zoningdistrict2 = zonedist
FROM lotzoneperorder AS b
WHERE
    a.bbl = b.bbl
    AND row_number = 2
    AND ROUND(perbblgeom::numeric, 2) >= 10;

UPDATE dcp_zoning_taxlot a
SET zoningdistrict3 = zonedist
FROM lotzoneperorder AS b
WHERE
    a.bbl = b.bbl
    AND row_number = 3
    AND ROUND(perbblgeom::numeric, 2) >= 10;

UPDATE dcp_zoning_taxlot a
SET zoningdistrict4 = zonedist
FROM lotzoneperorder AS b
WHERE
    a.bbl = b.bbl
    AND row_number = 4
    AND ROUND(perbblgeom::numeric, 2) >= 10;
