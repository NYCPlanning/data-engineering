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
FROM facdb_base AS a
LEFT JOIN lookup_agency AS b
    ON a.opabbrev = b.agencyabbrev
LEFT JOIN lookup_agency AS c
    ON a.overabbrev = c.agencyabbrev;
