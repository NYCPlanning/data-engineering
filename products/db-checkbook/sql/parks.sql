-- assign geometries to capital projects using Parks Properties dataset

WITH master AS (
    SELECT 
        a.fms_id,
        a.agency, 
        a.budget_code, 
        a.contract_purpose, 
        a.has_geometry, 
        a.geometry, 
        b.EAPPLY, 
        b.ADDRESS, 
        b.NAME311, 
        b.SIGNNAME, 
        b.multipolygon
FROM csdb a
JOIN parks b ON (
        (upper(a.agency) LIKE '%DEPARTMENT OF PARKS AND RECREATION%' OR
        upper(a.agency) LIKE '%DPR%')
        AND a.geometry IS NULL
        AND (
            upper(a.budget_code) LIKE '%' || upper(b.EAPPLY) || '%' OR
            upper(a.budget_code) LIKE '%' || upper(b.ADDRESS) || '%' OR
            upper(a.budget_code) LIKE '%' || upper(b.NAME311) || '%' OR
            upper(a.budget_code) LIKE '%' || upper(b.SIGNNAME) || '%' OR
            upper(a.contract_purpose) LIKE '%' || upper(b.EAPPLY) || '%' OR 
            upper(a.contract_purpose) LIKE '%' || upper(b.ADDRESS) || '%' OR
            upper(a.contract_purpose) LIKE '%' || upper(b.NAME311) || '%' OR
            upper(a.contract_purpose) LIKE '%' || upper(b.SIGNNAME) || '%'
        )
    )

UPDATE csdb 
SET 
    geometry = master.multipolygon,
    has_geometry = 1,
    final_category = 'Fixed Asset'
FROM master
WHERE
    csdb.fms_id = master.fms_id;