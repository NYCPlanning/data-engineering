CREATE TEMP TABLE tmp (
    year text,
    borough character varying,
    hs bigint
);

\COPY tmp FROM 'output/_sca_e_projections_by_boro.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS :"NAME" CASCADE;
SELECT *
INTO :"NAME"
FROM tmp;
