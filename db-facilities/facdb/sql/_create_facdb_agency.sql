DROP TABLE IF EXISTS facdb_agency;
SELECT
    a.uid,
    a.opname,
    a.opabbrev,
    b.optype,
    a.overabbrev,
    c.overagency,
    c.overlevel
INTO facdb_agency
FROM facdb_base a
LEFT JOIN lookup_agency b
ON a.opabbrev = b.agencyabbrev
LEFT JOIN lookup_agency c
ON a.overabbrev = c.agencyabbrev;
