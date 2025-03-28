-- set the pluto map id based on the following criteria

-- 1 In PLUTO Data File and DOF Modified DTM Tax Block and Lot
-- Clipped to the Shoreline File.
-- 2 In PLUTO Data File Only.
-- 3 In DOF Modified DTM Tax Block and Lot Clipped to the
-- Shoreline File but NOT in PLUTO.
-- 4 In PLUTO Data File and in DOF Modified DTM File but NOT in
-- DOF Modified DTM Tax Block and Lot Clipped to the Shoreline
-- File, therefore tshe tax lot is totally under water.
-- 5 In DOF Modified DTM but NOT in PLUTO, therefore the tax lot
-- is totally under water.

-- set the mappluto ID based on the critera above
-- values can overwrite each other
UPDATE pluto
SET plutomapid = '1'
WHERE
    geom IS NOT NULL
    AND plutomapid IS NULL;

UPDATE pluto
SET plutomapid = '2'
WHERE geom IS NULL;

DROP TABLE IF EXISTS dof_shoreline_subdivide;
SELECT ST_SUBDIVIDE(ST_MAKEVALID(geom), 100) AS geom
INTO dof_shoreline_subdivide
FROM (
    SELECT ST_UNION(geom) AS geom
    FROM dof_shoreline
) AS a;
