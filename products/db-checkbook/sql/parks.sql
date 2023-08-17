-- assign geometries to capital projects using Parks Properties dataset

WITH csdb_parks AS (
    SELECT 
        csdb.fms_id,
        csdb.agency, 
        csdb.budget_code, 
        csdb.contract_purpose, 
        csdb.has_geometry, 
        csdb.geometry, 
        parks.signname, 
        parks.eapply,
        parks."WKT"
    FROM csdb
    JOIN parks ON  
        (upper(csdb.budget_code) LIKE '%' || upper(parks.signname) || '%' OR
        upper(csdb.contract_purpose) LIKE '%' || upper(parks.signname) || '%') OR
        (upper(csdb.budget_code) LIKE '%' || upper(parks.eapply) || '%' OR
        upper(csdb.contract_purpose) LIKE '%' || upper(parks.eapply) || '%')
    WHERE 
        (upper(csdb.agency) LIKE '%DEPARTMENT OF PARKS AND RECREATION%' OR upper(csdb.agency) LIKE '%DPR%') AND
        csdb.geometry IS NULL AND
        upper(parks.signname) <> 'PARK' AND
        upper(parks.signname) <> 'LOT' AND
        upper(parks.signname) <> 'GARDEN' AND
        upper(parks.signname) <> 'TRIANGLE' AND
        upper(parks.signname) <> 'SITTING AREA' AND 
        upper(parks.eapply) <> 'PARK' AND
        upper(parks.eapply) <> 'LOT' AND
        upper(parks.eapply) <> 'GARDEN' AND
        upper(parks.eapply) <> 'TRIANGLE' AND
        upper(parks.eapply) <> 'SITTING AREA'
)
UPDATE csdb 
SET 
    geometry = csdb_parks."WKT",
    has_geometry = true
FROM csdb_parks
WHERE csdb.fms_id = csdb_parks.fms_id AND csdb.geometry IS NULL;

