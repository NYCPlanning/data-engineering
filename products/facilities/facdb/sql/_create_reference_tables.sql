DROP TABLE IF EXISTS lookup_boro;
CREATE TABLE lookup_boro (
    boro text,
    boroname text,
    borocode integer
);
\COPY lookup_boro FROM 'facdb/data/lookup_boro.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS lookup_classification;
CREATE TABLE lookup_classification (
    facsubgrp text,
    facgroup text,
    facdomain text,
    servarea text
);
\COPY lookup_classification FROM 'facdb/data/lookup_classification.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS lookup_agency;
CREATE TABLE lookup_agency (
    agencyabbrev text,
    overagency text,
    overlevel text,
    optype text
);
\COPY lookup_agency FROM 'facdb/data/lookup_agency.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS manual_corrections;
CREATE TABLE manual_corrections (
    uid text,
    field text,
    old_value text,
    new_value text
);
\COPY manual_corrections FROM 'facdb/data/manual_corrections.csv' DELIMITER ',' CSV HEADER;
