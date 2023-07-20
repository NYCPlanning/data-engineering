CREATE SCHEMA IF NOT EXISTS pff_acs;
DROP TABLE IF EXISTS pff_acs.:"TABLE_NAME";

CREATE TEMP TABLE tmp (
    census_geoid text,
    labs_geoid text,
    geotype text,
    labs_geotype text,
    pff_variable text,
    c double precision,
    e double precision,
    m double precision,
    p double precision,
    z double precision,
    domain text
);

\COPY tmp FROM PSTDIN WITH DELIMITER ',' CSV HEADER;

SELECT * INTO pff_acs.:"TABLE_NAME" FROM tmp;
