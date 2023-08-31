-- flagging records that are within an area that has recently been rezoned
UPDATE dcp_zoning_taxlot a
SET inzonechange = 'Y'
FROM dof_dtm AS b, dcp_zoningmapamendments AS c
WHERE
    a.bbl::TEXT = b.bbl::TEXT
    AND ST_INTERSECTS(b.geom, c.geom)
    AND c.effective::DATE > CURRENT_DATE - INTERVAL '2 months';
