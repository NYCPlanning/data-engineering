-- assign geometries to capital projects using Parks Properties dataset

WITH master AS (
    SELECT 
        csdb.fms_id,
        csdb.agency, 
        csdb.budget_code, 
        csdb.contract_purpose, 
        csdb.has_geometry, 
        csdb.geometry, 
        parks.signname, 
        parks.multipolygon
    FROM csdb
    JOIN parks ON (upper(csdb.agency) LIKE '%DEPARTMENT OF PARKS AND RECREATION%' OR upper(csdb.agency) LIKE '%DPR%') AND 
            (upper(csdb.budget_code) LIKE '%' || upper(parks.signname) || '%' OR
            upper(csdb.contract_purpose) LIKE '%' || upper(parks.signname) || '%') AND
            csdb.geometry IS NULL AND
            upper(parks.signname) <> 'PARK' AND
            upper(parks.signname) <> 'LOT' AND
            upper(parks.signname) <> 'GARDEN' AND
            upper(parks.signname) <> 'TRIANGLE' AND
            upper(parks.signname) <> 'SITTING AREA'
)
UPDATE csdb 
SET 
    geometry = master.multipolygon,
    has_geometry = true,
    final_category = 'Fixed Asset'
FROM master
WHERE
    csdb.fms_id = master.fms_id
    AND csdb.geometry IS NULL;

