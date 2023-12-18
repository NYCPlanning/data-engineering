CREATE TEMP TABLE atypical_roadways(
    streetname text,
    segmentid text,
    streetwidth_min text,
    streetwidth_max text,
    right_zipcode text,
    left_zipcode text,
    borocode integer,
    nodelevelf text,
    nodelevelt text,
    featuretyp text,
    trafdir text,
    number_total_lanes text,
    bikelane text,
    geom geometry(LineString,4326)
);

\COPY atypical_roadways FROM PSTDIN DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS atypical_roadways.:"VERSION" CASCADE;
SELECT * INTO atypical_roadways.:"VERSION" FROM atypical_roadways;

DROP VIEW IF EXISTS atypical_roadways.latest;
CREATE VIEW atypical_roadways.latest AS (
    SELECT :'VERSION' as v, * 
    FROM atypical_roadways.:"VERSION"
);