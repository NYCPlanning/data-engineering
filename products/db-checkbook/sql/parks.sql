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
        parks.name311,
        parks.address,
        parks."WKT"
    FROM csdb
    JOIN parks ON
        ---  signname 
        (upper(csdb.budget_code) LIKE '%' || upper(parks.signname) || '%' OR
        upper(csdb.contract_purpose) LIKE '%' || upper(parks.signname) || '%') OR
        --- eapply
        (upper(csdb.budget_code) LIKE '%' || upper(parks.eapply) || '%' OR
        upper(csdb.contract_purpose) LIKE '%' || upper(parks.eapply) || '%') OR
        --- name311
        (upper(csdb.budget_code) LIKE '%' || upper(parks.name311) || '%' OR
        upper(csdb.contract_purpose) LIKE '%' || upper(parks.name311) || '%') OR
        --- address
        (upper(csdb.budget_code) LIKE '%' || upper(parks.address) || '%' OR
        upper(csdb.contract_purpose) LIKE '%' || upper(parks.address) || '%')
    WHERE 
        (upper(csdb.agency) LIKE '%DEPARTMENT OF PARKS AND RECREATION%' OR upper(csdb.agency) LIKE '%DPR%') AND
        csdb.geometry IS NULL AND
        ---  signname 
        upper(parks.signname) <> 'PARK' AND
        upper(parks.signname) <> 'LOT' AND
        upper(parks.signname) <> 'GARDEN' AND
        upper(parks.signname) <> 'TRIANGLE' AND
        upper(parks.signname) <> 'SITTING AREA' AND 
        --- eapply
        upper(parks.eapply) <> 'PARK' AND
        upper(parks.eapply) <> 'LOT' AND
        upper(parks.eapply) <> 'GARDEN' AND
        upper(parks.eapply) <> 'TRIANGLE' AND
        upper(parks.eapply) <> 'SITTING AREA' AND
        --- name311
        upper(parks.name311) <> 'PARK' AND
        upper(parks.name311) <> 'LOT' AND
        upper(parks.name311) <> 'GARDEN' AND
        upper(parks.name311) <> 'TRIANGLE' AND
        upper(parks.name311) <> 'SITTING AREA'
        --- don't think we need to check address...
)
UPDATE csdb 
SET 
    geometry = csdb_parks."WKT",
    has_geometry = true
FROM csdb_parks
WHERE csdb.fms_id = csdb_parks.fms_id AND csdb.geometry IS NULL;

