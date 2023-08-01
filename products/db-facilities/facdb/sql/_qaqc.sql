--Add mapped column to dcp_facilities_with_unmapped
ALTER TABLE dcp_facilities_with_unmapped ADD COLUMN IF NOT EXISTS mapped boolean;
UPDATE dcp_facilities_with_unmapped
SET  mapped = (latitude::numeric != 0 AND longitude::numeric !=0);

-- QC consistency in operator information
DROP TABLE IF EXISTS qc_operator;
WITH
new as (
	SELECT opabbrev, opname, optype, datasource, count(*) as count_new
	FROM facdb
	group by opabbrev, opname, optype, datasource
),
old as (
	SELECT opabbrev, opname, optype, datasource, count(*) as count_old
	FROM dcp_facilities_with_unmapped
	group by opabbrev, opname, optype, datasource
)
select
	coalesce(a.opabbrev, b.opabbrev) as opabbrev,
	coalesce(a.opname, b.opname) as opname,
	coalesce(a.optype, b.optype) as optype,
	coalesce(a.datasource, b.datasource) as datasource,
	b.count_old,  a.count_new - b.count_old as diff
INTO qc_operator
from new a join old b
on a.opabbrev = b.opabbrev
and a.opname = b.opname
and a.optype = b.optype
and a.datasource = b.datasource;

-- QC consistency in oversight information
DROP TABLE IF EXISTS qc_oversight;
with
new as (
	SELECT overabbrev, overagency, overlevel, datasource, count(*) as count_new
	FROM facdb
	group by overabbrev, overagency, overlevel, datasource
),
old as (
	SELECT overabbrev, overagency, overlevel, datasource, count(*) as count_old
	FROM dcp_facilities_with_unmapped
	group by overabbrev, overagency, overlevel, datasource
)
select
	coalesce(a.overabbrev, b.overabbrev) as overabbrev,
	coalesce(a.overagency, b.overagency) as overagency,
	coalesce(a.overlevel, b.overlevel) as optype,
	coalesce(a.datasource, b.datasource) as datasource,
	b.count_old,  a.count_new - b.count_old as diff
INTO qc_oversight
from new a
join old b
on a.overabbrev = b.overabbrev
and a.overagency = b.overagency
and a.overlevel = b.overlevel
and a.datasource = b.datasource;

-- QC consistency in grouping information
DROP TABLE IF EXISTS qc_classification;
with
new as (
	SELECT facdomain, facgroup, facsubgrp, servarea, count(*) as count_new
	FROM facdb
	group by facdomain, facgroup, facsubgrp, servarea
),
old as (
	SELECT facdomain, facgroup, facsubgrp, servarea, count(*) as count_old
	FROM dcp_facilities_with_unmapped
	group by facdomain, facgroup, facsubgrp, servarea
)
select
	coalesce(a.facdomain, b.facdomain) as facdomain,
	coalesce(a.facgroup, b.facgroup) as facgroup,
	coalesce(a.facsubgrp, b.facsubgrp) as facsubgrp,
	coalesce(a.servarea, b.servarea) as servarea,
	b.count_old,  a.count_new - b.count_old as diff
INTO qc_classification
from new a
join old b
on a.facdomain = b.facdomain
and a.facgroup = b.facgroup
and a.facsubgrp = b.facsubgrp
and a.servarea = b.servarea;

-- make sure capcaity types are consistent
DROP TABLE IF EXISTS qc_captype;
with
new as (
	SELECT captype, sum(capacity::numeric)::integer as sum_new
	FROM facdb
	GROUP BY captype
),
old as (
	SELECT captype, sum(capacity::numeric)::integer as sum_old
	FROM dcp_facilities_with_unmapped
	GROUP BY captype
)
select a.captype, a.sum_new, b.sum_old, a.sum_new - b.sum_old as diff
INTO qc_captype
from new a
join old b
on a.captype = b.captype;

DROP TABLE IF EXISTS qc_mapped;
WITH
geom_new as (
	SELECT facdomain, facgroup, facsubgrp, factype, datasource,
	count(*) as count_new,
	sum((geom is NOT null)::integer) as with_geom_new
	from facdb
	group by facdomain, facgroup, facsubgrp, factype, datasource
),
geom_old as (
	SELECT facdomain, facgroup, facsubgrp, factype, datasource,
	count(*) as count_old,
	count(*) FILTER (WHERE mapped=TRUE) as with_geom_old
	from dcp_facilities_with_unmapped
	group by facdomain, facgroup, facsubgrp, factype, datasource
)
select
	coalesce(a.facdomain, b.facdomain) as facdomain,
	coalesce(a.facgroup, b.facgroup) as facgroup,
	coalesce(a.facsubgrp, b.facsubgrp) as facsubgrp,
	coalesce(a.factype, b.factype) as factype,
	coalesce(a.datasource, b.datasource) as datasource,
	coalesce(b.count_old, 0) as count_old,
	coalesce(a.count_new, 0) as count_new,
	coalesce(a.with_geom_new, 0) as with_geom_new,
	coalesce(b.with_geom_old, 0) as with_geom_old
INTO qc_mapped
from geom_new a
FULL join geom_old b
on a.facdomain = b.facdomain
AND a.facgroup = b.facgroup
AND a.facsubgrp = b.facsubgrp
AND a.factype = b.factype
AND a.datasource = b.datasource;

-- report Change in distribution of number of records by fac subgroup / group / domain between current and previous version
DROP TABLE IF EXISTS qc_diff;
select
	coalesce(a.facdomain, b.facdomain) as facdomain,
	coalesce(a.facgroup, b.facgroup) as facgroup,
	coalesce(a.facsubgrp, b.facsubgrp) as facsubgrp,
	coalesce(a.factype, b.factype) as factype,
	coalesce(a.datasource, b.datasource) as datasource,
	coalesce(count_old, 0) as count_old,
	coalesce(count_new, 0) as count_new,
	coalesce(count_new, 0) - coalesce(count_old, 0) as diff
INTO qc_diff
FROM
(
	select facdomain, facgroup, facsubgrp, factype, datasource, coalesce(count(*),0) as count_new
	from facdb
	where geom is not null
	group by facdomain, facgroup, facsubgrp, factype, datasource
) a FULL JOIN
(	select facdomain, facgroup, facsubgrp, factype, datasource, coalesce(count(*),0) as count_old
	from dcp_facilities_with_unmapped
	where mapped is true
	group by facdomain, facgroup, facsubgrp, factype, datasource
) b
ON a.facdomain = b.facdomain
and a.facgroup = b.facgroup
and a.facsubgrp = b.facsubgrp
and a.factype = b.factype
and a.datasource = b.datasource
order by facdomain, facgroup, facsubgrp, factype;

-- QC Number of records source vs facdb
DROP TABLE IF EXISTS qc_recordcounts;
SELECT
	a.datasource,
	a.raw_record_counts,
	b.final_record_counts,
	a.raw_record_counts-b.final_record_counts as diff
INTO qc_recordcounts
FROM (
	SELECT source as datasource, count(*) as raw_record_counts
	FROM facdb_base GROUP BY source
) a LEFT JOIN (
	SELECT datasource, count(*) as final_record_counts
	FROM facdb GROUP BY datasource
) b ON a.datasource=b.datasource
ORDER BY diff DESC;

-- QC on bins by subgroup
DROP TABLE IF EXISTS qc_subgrpbins;
SELECT
	facsubgrp,
	count(*) as count_total,
	count(distinct bin) as count_distinct_bin,
	count(*) filter (where bin is not null) - count(distinct bin) as count_repeat_bin,
	count(*) filter (where bin is null) as count_null_bin,
	count(*) filter (where bin::text like '%000000') as count_million_bin,
	count(*) filter (where geom is null) as count_wo_geom
INTO qc_subgrpbins
FROM facdb
GROUP BY facsubgrp
ORDER BY count_repeat_bin DESC;
