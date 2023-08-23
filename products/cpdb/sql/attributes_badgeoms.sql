-- remove faulty geometries from attributes table

-- Remove
UPDATE cpdb_dcpattributes
SET geom = NULL
WHERE maprojid in (SELECT DISTINCT maprojid FROM cpdb_badgeoms)
AND (geomsource <> 'dpr' OR geomsource <> 'dot' OR geomsource <> 'ddc')
;

-- Remove from agency data
UPDATE cpdb_dcpattributes
SET geom = NULL
WHERE 
(
    (maprojid IN (
        SELECT DISTINCT maprojid
        FROM dcp_cpdb_agencyverified
        WHERE mappable = 'No - Can never be mapped'
        )
    )
OR 
    (maprojid IN (
        SELECT DISTINCT maprojid
        FROM dcp_cpdb_agencyverified
        WHERE mappable = 'No - Can be in future'
        )
    AND NOT geomsource IN ('dpr','dot','ddc','edc')
    )
)
AND geom IS NOT NULL
;