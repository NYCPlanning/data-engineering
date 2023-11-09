WITH dof_dtm_full AS (
    SELECT * 
    FROM dof_dtm
)
SELECT
    bbl,
    COALESCE(boro::text, LEFT(bbl::text, 1)) AS boro,
    COALESCE(block::text, SUBSTRING(bbl::text, 2, 5)) AS block,
    COALESCE(lot::text, SUBSTRING(bbl::text, 7, 4)) AS lot,
    ST_MULTI(ST_UNION(ST_MAKEVALID(wkb_geometry))) AS geom
FROM dof_dtm_full
GROUP BY bbl, boro, block, lot;
