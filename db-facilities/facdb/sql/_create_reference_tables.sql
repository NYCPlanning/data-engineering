DROP TABLE IF EXISTS lookup_boro;
CREATE TABLE lookup_boro (
    boro TEXT,
    boroname TEXT,
    borocode INTEGER
);
\COPY lookup_boro FROM 'facdb/data/lookup_boro.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS lookup_classification;
CREATE TABLE lookup_classification (
    facsubgrp TEXT,
    facgroup TEXT,
    facdomain TEXT,
    servarea TEXT
);
\COPY lookup_classification FROM 'facdb/data/lookup_classification.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS lookup_agency;
CREATE TABLE lookup_agency (
    agencyabbrev TEXT,
    overagency TEXT,
    overlevel TEXT,
    optype TEXT
);
\COPY lookup_agency FROM 'facdb/data/lookup_agency.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS manual_corrections;
CREATE TABLE manual_corrections (
    uid TEXT,
    field TEXT,
    old_value TEXT,
    new_value TEXT
);
\COPY manual_corrections FROM 'facdb/data/manual_corrections.csv' DELIMITER ',' CSV HEADER;
