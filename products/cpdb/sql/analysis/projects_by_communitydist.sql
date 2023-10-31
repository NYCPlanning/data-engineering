-- get actual and projected spending by community district for individual projects
DROP TABLE IF EXISTS projects_by_communitydist;
CREATE TABLE projects_by_communitydist AS (
    WITH fms_bud AS (
        SELECT
            typc,
            maprojid,
            SUBSTRING(budgetline FROM '([A-Z]+)(-{1})') AS budget_proj_type,
            SUM(plannedcommit_total) AS amt
        FROM ccp_commitments
        GROUP BY
            typc,
            maprojid,
            budget_proj_type
    ),

    fmsmerge AS (
        SELECT
            a.*,
            b.description,
            b.geom
        FROM fms_bud AS a,
            cpdb_dcpattributes AS b
        WHERE a.maprojid = b.maprojid
        ORDER BY maprojid
    ),

    per_pt AS (
        SELECT
            maprojid,
            typc,
            budget_proj_type,
            description,
            SUM(amt) / ST_NUMGEOMETRIES(geom) AS amt_per_pt,
            (ST_DUMP(geom)).geom AS geom
        FROM fmsmerge
        WHERE
            geom IS NOT NULL
            AND ST_GEOMETRYTYPE(geom) = 'ST_MultiPoint'
        GROUP BY
            maprojid,
            typc,
            budget_proj_type,
            description,
            geom
    ),

    cd_pt AS ( -- join community districts with point and sum spending
        -- assumes equal spending at each point
        SELECT
            a.borocd,
            b.maprojid,
            b.typc,
            b.budget_proj_type,
            b.description,
            SUM(b.amt_per_pt) AS amt_pt
        FROM dcp_cdboundaries AS a
        LEFT JOIN per_pt AS b ON ST_WITHIN(b.geom, a.wkb_geometry)
        GROUP BY a.borocd, b.maprojid, b.typc, b.budget_proj_type, b.description
    ),

    per_poly AS (
        SELECT
            maprojid,
            typc,
            budget_proj_type,
            description,
            SUM(amt) AS total_amt,
            ST_AREA(geom) AS total_area,
            (ST_DUMP(geom)).geom AS geom
        FROM fmsmerge
        WHERE
            geom IS NOT NULL
            AND ST_GEOMETRYTYPE(geom) = 'ST_MultiPolygon'
        GROUP BY
            maprojid,
            typc,
            budget_proj_type,
            description,
            geom
    ),

    cd_poly AS ( -- join community districts with polygons and sum spending
        -- divides spending proportional to size of the multiple site polygons
        -- then if there is a polygon that crosses multiple community districts
        -- divides that spending again proportional to size of each intersection
        SELECT
            a.borocd,
            c.maprojid,
            c.typc,
            c.budget_proj_type,
            c.description,
            SUM(
                c.total_amt
                * (ST_AREA(c.geom) / c.total_area)
                * ST_AREA(ST_INTERSECTION(c.geom, a.wkb_geometry)) / ST_AREA(c.geom)
            ) AS amt_poly
        FROM dcp_cdboundaries AS a
        LEFT JOIN per_poly AS c ON ST_INTERSECTS(c.geom, a.wkb_geometry)
        WHERE
            ST_ISVALID(a.wkb_geometry) = 't'
            AND ST_ISVALID(c.geom) = 't'
        GROUP BY
            a.borocd,
            c.maprojid,
            c.typc,
            c.budget_proj_type,
            c.description
    ),

    cd_all AS (
        SELECT
            borocd,
            maprojid,
            typc,
            budget_proj_type,
            description,
            amt_pt AS amt
        FROM cd_pt
        UNION
        SELECT
            borocd,
            maprojid,
            typc,
            budget_proj_type,
            description,
            amt_poly AS amt
        FROM cd_poly
    )

    SELECT
        a.*,
        b.typecategory,
        b.geomsource,
        b.dataname,
        b.datasource
    --ST_Multi(ST_Buffer(b.geom, 10)) AS geom
    FROM cd_all AS a
    LEFT JOIN (
        SELECT DISTINCT
            maprojid,
            typecategory,
            geomsource,
            dataname,
            datasource
        FROM cpdb_dcpattributes
    ) AS b
        ON a.maprojid = b.maprojid
);
