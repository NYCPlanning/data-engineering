CREATE TEMP TABLE centroid (
    geoid character varying,
    centroid geometry(Point,4326)
);


\COPY centroid FROM PSTDIN DELIMITER ',' CSV HEADER;

CREATE TEMP TABLE tmp as (
    SELECT 
        geoid,
        centroid
    FROM centroid
    WHERE geoid in (
        SELECT residential_geoid FROM ctpp_journey_to_work.latest
        UNION
        SELECT work_geoid FROM ctpp_journey_to_work.latest
    )
);

DROP TABLE IF EXISTS :NAME.:"VERSION" CASCADE;
SELECT *
INTO :NAME.:"VERSION"
FROM tmp;

DROP VIEW IF EXISTS :NAME.latest;
CREATE VIEW :NAME.latest AS (
    SELECT :'VERSION' as v, * 
    FROM :NAME.:"VERSION"
);