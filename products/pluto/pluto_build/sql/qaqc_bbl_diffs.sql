DROP TABLE IF EXISTS qaqc_bbl_diffs;
CREATE TABLE qaqc_bbl_diffs (
    source_version text,
    bbl text,
    type text
);

-- add new bbl records
INSERT INTO qaqc_bbl_diffs (
    SELECT
        :'VERSION' AS source_version,
        ap.bbl::float::bigint::text AS bbl,
        'new' AS type
    FROM archive_pluto AS ap
    LEFT JOIN previous_pluto AS pp
        ON ap.bbl::float::bigint = pp.bbl::float::bigint
    WHERE pp.bbl IS NULL
);

-- add vanished bbl records
INSERT INTO qaqc_bbl_diffs (
    SELECT
        :'VERSION_PREV' AS source_version,
        pp.bbl::float::bigint::text AS bbl,
        'vanished' AS type
    FROM previous_pluto AS pp
    LEFT JOIN archive_pluto AS ap
        ON ap.bbl::float::bigint = pp.bbl::float::bigint
    WHERE ap.bbl IS NULL
);
