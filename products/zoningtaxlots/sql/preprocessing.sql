DROP TABLE IF EXISTS dof_dtm_tmp;
CREATE TABLE dof_dtm_tmp AS (
    WITH coalesced AS (
        SELECT
            bbl,
            COALESCE(boro::text, LEFT(bbl::text, 1)) AS boro,
            COALESCE(block::text, SUBSTRING(bbl::text, 2, 5)) AS block,
            COALESCE(lot::text, SUBSTRING(bbl::text, 7, 4)) AS lot,
            geom
        FROM dof_dtm
    )

    SELECT
        ROW_NUMBER() OVER () AS id,
        bbl,
        boro,
        block,
        lot,
        ST_MULTI(ST_UNION(ST_MAKEVALID(geom))) AS geom
    FROM coalesced
    GROUP BY bbl, boro, block, lot
);

DROP TABLE IF EXISTS dof_dtm;
ALTER TABLE dof_dtm_tmp
RENAME TO dof_dtm;

CREATE INDEX dof_dtm_wkb_geometry_geom_idx ON dof_dtm USING gist (geom gist_geometry_ops_2d);
