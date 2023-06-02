DROP TABLE IF EXISTS lookup_occ;
CREATE TABLE lookup_occ(
    dob_occ text,
    occ text
); 
\COPY lookup_occ FROM 'data/lookup_occ.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS lookup_ownership;
CREATE TABLE lookup_ownership (
    cityowned text,
    ownertype text,
    nonprofit text,
    ownership text
);
\COPY lookup_ownership FROM 'data/lookup_ownership.csv' DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS _manual_corrections;
CREATE TABLE _manual_corrections (
    job_number text,
    field text,
    old_value text,
    new_value text,
    reason text,
    edited_date text,
    editor text
);
\COPY _manual_corrections FROM 'data/manual_corrections.csv' DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS CORR_hny_matches;
CREATE TABLE CORR_hny_matches (
    hny_id text,
    job_number text,
    hny_project_id text,
    action text
);
\COPY CORR_hny_matches FROM 'data/CORR_hny_matches.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS housing_input_hny;
CREATE TABLE housing_input_hny (
    job_number text,
    hny_id text
);
\COPY housing_input_hny FROM 'data/housing_input_hny.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS census_units10;
CREATE TABLE census_units10 (
    CenBlock10 text,
    CenTract10 text,
    NTA10 text,
    PUMA10 text,
    CenUnits10 numeric
);
\COPY census_units10 FROM 'data/census_units10.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS census_units10adj;
CREATE TABLE census_units10adj (
    BCT2010 text,
    CenTract10 text,
    NTA10 text,
    PUMA10 text,
    AdjUnits10 numeric
);
\COPY census_units10adj FROM 'data/census_units10adj.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS lookup_geo;
CREATE TABLE lookup_geo (
    boro text,
    borocode text,
    fips_boro text,
    ctcb2010 text,
    ct2010 text,
    bctcb2010 text,
    bct2010 text,
    puma text,
    pumaname text,
    nta text,
    ntaname text,
    commntydst text,
    councildst text
);
\COPY lookup_geo FROM 'data/lookup_geo.csv' DELIMITER ',' CSV HEADER;
