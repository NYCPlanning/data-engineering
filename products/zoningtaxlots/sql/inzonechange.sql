-- flagging records that are within an area that has recently been rezoned
UPDATE dcp_zoning_taxlot a
SET inzonechange = 'Y'
FROM dof_dtm AS b, dcp_zoningmapamendments AS c
WHERE
    a.dtm_id = b.id
    AND st_intersects(b.geom, c.geom)
    AND c.effective::date > current_date - interval '2 months';
