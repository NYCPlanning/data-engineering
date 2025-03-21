CREATE SCHEMA IF NOT EXISTS :NAME;

CREATE TEMP TABLE tmp (
    bldg_id character varying,
    org_id character varying,
    bldg_id_additional character varying,
    title character varying,
    at_scale_year character varying,
    school_year character varying,
    at_scale_enroll character varying,
    vote_date character varying
);

\COPY tmp FROM PSTDIN DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS :NAME.:"VERSION" CASCADE;
SELECT
    a.bldg_id,
    a.org_id,
    a.bldg_id_additional,
    a.title,
    a.at_scale_year,
    b.url,
    a.at_scale_enroll,
    TO_CHAR(TO_DATE(a.vote_date, 'MM/DD/YYYY'), 'YYYY-MM-DD') as vote_date
INTO :NAME.:"VERSION"
FROM tmp a
JOIN doe_pepmeetingurls b
ON a.school_year = b.school_year
AND TO_CHAR(TO_DATE(a.vote_date, 'MM/DD/YYYY'), 'YYYY-MM-DD') = b.date
;

DROP VIEW IF EXISTS :NAME.latest;
CREATE VIEW :NAME.latest AS (
    SELECT :'VERSION' as v, * 
    FROM :NAME.:"VERSION"
);