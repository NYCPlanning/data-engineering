--Add mapped column to dcp_facilities_with_unmapped
ALTER TABLE dcp_facilities_with_unmapped ADD COLUMN IF NOT EXISTS mapped boolean;
UPDATE dcp_facilities_with_unmapped
SET mapped = (latitude != 0 AND longitude != 0);

-- QC consistency in operator information
DROP TABLE IF EXISTS qc_operator;
WITH
new AS (
    SELECT
        opabbrev,
        opname,
        optype,
        datasource,
        count(*) AS count_new
    FROM facdb
    GROUP BY opabbrev, opname, optype, datasource
),
old AS (
    SELECT
        opabbrev,
        opname,
        optype,
        datasource,
        count(*) AS count_old
    FROM dcp_facilities_with_unmapped
    GROUP BY opabbrev, opname, optype, datasource
)
SELECT
    coalesce(a.opabbrev, b.opabbrev) AS opabbrev,
    coalesce(a.opname, b.opname) AS opname,
    coalesce(a.optype, b.optype) AS optype,
    coalesce(a.datasource, b.datasource) AS datasource,
    b.count_old,
    a.count_new - b.count_old AS diff
INTO qc_operator
FROM new AS a INNER JOIN old AS b
    ON
        a.opabbrev = b.opabbrev
        AND a.opname = b.opname
        AND a.optype = b.optype
        AND a.datasource = b.datasource;

-- QC consistency in oversight information
DROP TABLE IF EXISTS qc_oversight;
WITH
new AS (
    SELECT
        overabbrev,
        overagency,
        overlevel,
        datasource,
        count(*) AS count_new
    FROM facdb
    GROUP BY overabbrev, overagency, overlevel, datasource
),
old AS (
    SELECT
        overabbrev,
        overagency,
        overlevel,
        datasource,
        count(*) AS count_old
    FROM dcp_facilities_with_unmapped
    GROUP BY overabbrev, overagency, overlevel, datasource
)
SELECT
    coalesce(a.overabbrev, b.overabbrev) AS overabbrev,
    coalesce(a.overagency, b.overagency) AS overagency,
    coalesce(a.overlevel, b.overlevel) AS optype,
    coalesce(a.datasource, b.datasource) AS datasource,
    b.count_old,
    a.count_new - b.count_old AS diff
INTO qc_oversight
FROM new AS a
INNER JOIN old AS b
    ON
        a.overabbrev = b.overabbrev
        AND a.overagency = b.overagency
        AND a.overlevel = b.overlevel
        AND a.datasource = b.datasource;

-- QC consistency in grouping information
DROP TABLE IF EXISTS qc_classification;
WITH
new AS (
    SELECT
        facdomain,
        facgroup,
        facsubgrp,
        servarea,
        count(*) AS count_new
    FROM facdb
    GROUP BY facdomain, facgroup, facsubgrp, servarea
),
old AS (
    SELECT
        facdomain,
        facgroup,
        facsubgrp,
        servarea,
        count(*) AS count_old
    FROM dcp_facilities_with_unmapped
    GROUP BY facdomain, facgroup, facsubgrp, servarea
)
SELECT
    coalesce(a.facdomain, b.facdomain) AS facdomain,
    coalesce(a.facgroup, b.facgroup) AS facgroup,
    coalesce(a.facsubgrp, b.facsubgrp) AS facsubgrp,
    coalesce(a.servarea, b.servarea) AS servarea,
    b.count_old,
    a.count_new - b.count_old AS diff
INTO qc_classification
FROM new AS a
INNER JOIN old AS b
    ON
        a.facdomain = b.facdomain
        AND a.facgroup = b.facgroup
        AND a.facsubgrp = b.facsubgrp
        AND a.servarea = b.servarea;

-- make sure capcaity types are consistent
DROP TABLE IF EXISTS qc_captype;
WITH
new AS (
    SELECT
        captype,
        sum(capacity::numeric)::integer AS sum_new
    FROM facdb
    GROUP BY captype
),
old AS (
    SELECT
        captype,
        sum(capacity) AS sum_old
    FROM dcp_facilities_with_unmapped
    GROUP BY captype
)
SELECT
    a.captype,
    a.sum_new,
    b.sum_old,
    a.sum_new - b.sum_old AS diff
INTO qc_captype
FROM new AS a
INNER JOIN old AS b
    ON a.captype = b.captype;

DROP TABLE IF EXISTS qc_mapped;
WITH
geom_new AS (
    SELECT
        facdomain,
        facgroup,
        facsubgrp,
        factype,
        datasource,
        count(*) AS count_new,
        sum((geom IS NOT NULL)::integer) AS with_geom_new
    FROM facdb
    GROUP BY facdomain, facgroup, facsubgrp, factype, datasource
),
geom_old AS (
    SELECT
        facdomain,
        facgroup,
        facsubgrp,
        factype,
        datasource,
        count(*) AS count_old,
        count(*) FILTER (WHERE mapped = TRUE) AS with_geom_old
    FROM dcp_facilities_with_unmapped
    GROUP BY facdomain, facgroup, facsubgrp, factype, datasource
)
SELECT
    coalesce(a.facdomain, b.facdomain) AS facdomain,
    coalesce(a.facgroup, b.facgroup) AS facgroup,
    coalesce(a.facsubgrp, b.facsubgrp) AS facsubgrp,
    coalesce(a.factype, b.factype) AS factype,
    coalesce(a.datasource, b.datasource) AS datasource,
    coalesce(b.count_old, 0) AS count_old,
    coalesce(a.count_new, 0) AS count_new,
    coalesce(a.with_geom_new, 0) AS with_geom_new,
    coalesce(b.with_geom_old, 0) AS with_geom_old
INTO qc_mapped
FROM geom_new AS a
FULL JOIN geom_old AS b
    ON
        a.facdomain = b.facdomain
        AND a.facgroup = b.facgroup
        AND a.facsubgrp = b.facsubgrp
        AND a.factype = b.factype
        AND a.datasource = b.datasource;

-- report Change in distribution of number of records by fac subgroup / group / domain between current and previous version
DROP TABLE IF EXISTS qc_diff;
SELECT
    coalesce(a.facdomain, b.facdomain) AS facdomain,
    coalesce(a.facgroup, b.facgroup) AS facgroup,
    coalesce(a.facsubgrp, b.facsubgrp) AS facsubgrp,
    coalesce(a.factype, b.factype) AS factype,
    coalesce(a.datasource, b.datasource) AS datasource,
    coalesce(count_old, 0) AS count_old,
    coalesce(count_new, 0) AS count_new,
    coalesce(count_new, 0) - coalesce(count_old, 0) AS diff
INTO qc_diff
FROM
    (
        SELECT
            facdomain,
            facgroup,
            facsubgrp,
            factype,
            datasource,
            coalesce(count(*), 0) AS count_new
        FROM facdb
        WHERE geom IS NOT NULL
        GROUP BY facdomain, facgroup, facsubgrp, factype, datasource
    ) AS a FULL JOIN
    (
        SELECT
            facdomain,
            facgroup,
            facsubgrp,
            factype,
            datasource,
            coalesce(count(*), 0) AS count_old
        FROM dcp_facilities_with_unmapped
        WHERE mapped IS TRUE
        GROUP BY facdomain, facgroup, facsubgrp, factype, datasource
    ) AS b
    ON
        a.facdomain = b.facdomain
        AND a.facgroup = b.facgroup
        AND a.facsubgrp = b.facsubgrp
        AND a.factype = b.factype
        AND a.datasource = b.datasource
ORDER BY facdomain, facgroup, facsubgrp, factype;

-- QC Number of records source vs facdb
DROP TABLE IF EXISTS qc_recordcounts;
SELECT
    a.datasource,
    a.raw_record_counts,
    b.final_record_counts,
    a.raw_record_counts - b.final_record_counts AS diff
INTO qc_recordcounts
FROM (
    SELECT
        source AS datasource,
        count(*) AS raw_record_counts
    FROM facdb_base
    GROUP BY source
) AS a LEFT JOIN (
    SELECT
        datasource,
        count(*) AS final_record_counts
    FROM facdb
    GROUP BY datasource
) AS b ON a.datasource = b.datasource
ORDER BY diff DESC;

-- QC on bins by subgroup
DROP TABLE IF EXISTS qc_subgrpbins;
SELECT
    facsubgrp,
    count(*) AS count_total,
    count(DISTINCT bin) AS count_distinct_bin,
    count(*) FILTER (WHERE bin IS NOT NULL) - count(DISTINCT bin) AS count_repeat_bin,
    count(*) FILTER (WHERE bin IS NULL) AS count_null_bin,
    count(*) FILTER (WHERE bin::text LIKE '%000000') AS count_million_bin,
    count(*) FILTER (WHERE geom IS NULL) AS count_wo_geom
INTO qc_subgrpbins
FROM facdb
GROUP BY facsubgrp
ORDER BY count_repeat_bin DESC;
