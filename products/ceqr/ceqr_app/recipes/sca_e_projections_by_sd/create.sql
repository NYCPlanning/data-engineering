CREATE TEMP TABLE tmp (
    school_year text,
    district character varying,
    subdistrict character varying,
    ps numeric,
    "is" numeric
);

\COPY tmp FROM 'output/_sca_e_projections_by_sd.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS :"NAME" CASCADE;
SELECT *
INTO :"NAME"
FROM tmp;
