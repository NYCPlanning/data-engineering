-- get spending by community district for individual projects
DROP TABLE IF EXISTS projects_by_communitydist_spending;
CREATE TABLE projects_by_communitydist_spending AS (
    WITH spending_merged AS (
        SELECT
            trim(left(capital_project, 12)) AS maprojid,
            sum(check_amount::double precision) AS total_spend
        FROM nycoc_checkbook
        -- WHERE LEFT(issue_date, 4)::double precision >= 2014
        GROUP BY trim(left(capital_project, 12))
    ),

    fmsmerge AS (
        SELECT
            a.*,
            b.description,
            b.geom
        FROM spending_merged AS a,
            cpdb_dcpattributes AS b
        WHERE a.maprojid = b.maprojid
        ORDER BY maprojid
    ),

    per_pt AS (
        SELECT
            maprojid,
            description,
            sum(total_spend) / st_numgeometries(geom) AS amt_per_pt,
            (st_dump(geom)).geom AS geom
        FROM fmsmerge
        WHERE
            geom IS NOT NULL
            AND st_geometrytype(geom) = 'ST_MultiPoint'
        GROUP BY
            maprojid,
            description,
            geom
    ),

    cd_pt AS ( -- join community districts with point and sum spending
        -- assumes equal spending at each point
        SELECT
            a.borocd,
            b.maprojid,
            b.description,
            sum(b.amt_per_pt) AS amt_pt
        FROM dcp_cdboundaries AS a
        LEFT JOIN per_pt AS b ON st_within(b.geom, a.wkb_geometry)
        GROUP BY a.borocd, b.maprojid, b.description
    ),

    per_poly AS (
        SELECT
            maprojid,
            description,
            sum(total_spend) AS total_amt,
            st_area(geom) AS total_area,
            (st_dump(geom)).geom AS geom
        FROM fmsmerge
        WHERE
            geom IS NOT NULL
            AND st_geometrytype(geom) = 'ST_MultiPolygon'
        GROUP BY
            maprojid,
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
            c.description,
            sum(
                c.total_amt
                * (st_area(c.geom) / c.total_area)
                * st_area(st_intersection(c.geom, a.wkb_geometry)) / st_area(c.geom)
            ) AS amt_poly
        FROM dcp_cdboundaries AS a
        LEFT JOIN per_poly AS c ON st_intersects(c.geom, a.wkb_geometry)
        WHERE
            st_isvalid(a.wkb_geometry) = 't'
            AND st_isvalid(c.geom) = 't'
        GROUP BY
            a.borocd,
            c.maprojid,
            c.description
    ),

    cd_all AS (
        SELECT
            borocd,
            maprojid,
            description,
            amt_pt AS amt
        FROM cd_pt
        UNION
        SELECT
            borocd,
            maprojid,
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
