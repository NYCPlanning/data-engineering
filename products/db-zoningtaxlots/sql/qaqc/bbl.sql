delete from dcp_zoningtaxlots.qaqc_bbl
where version =: 'VERSION';

insert into dcp_zoningtaxlots.qaqc_bbl (
    select
        sum(case when bblnew is null then 1 else 0 end) as removed,
        sum(case when bblold is null then 1 else 0 end) as added,
        : 'VERSION' as version,
        : 'VERSION_PREV' as version_prev
    from (
        select
            a.bbl as bblnew,
            b.bbl as bblold
        from dcp_zoningtaxlots.:"VERSION" a
    FULL OUTER JOIN dcp_zoningtaxlots.:"VERSION_PREV" b
    ON b.bbl=a.bbl
    ) as c
);
