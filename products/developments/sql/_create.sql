DROP TABLE IF EXISTS lookup_occ;
CREATE TABLE lookup_occ (
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

DROP TABLE IF EXISTS lookup_now_types;
CREATE TABLE lookup_now_types (
    code text,
    description text
);
\COPY lookup_now_types FROM 'data/lookup_now_types.csv' DELIMITER ',' CSV HEADER;

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

DROP TABLE IF EXISTS corr_hny_matches;
CREATE TABLE corr_hny_matches (
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
