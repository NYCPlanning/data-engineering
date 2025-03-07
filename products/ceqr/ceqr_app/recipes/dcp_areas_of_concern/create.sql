/*
DESCRIPTION:
    Read data from PSTDIN and transfer to EDM database
INPUTS: 
	PSTDIN >> 
    TEMP temp(name text,
        geom geometry
    ) 
OUTPUTS:
	dcp_areas_of_concern.latest(
        Same schema as tmp
    )
*/

CREATE TEMP TABLE tmp (
    name text,
    geom geometry(MultiPolygon,4326)
);

\COPY tmp FROM PSTDIN DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS :NAME.:"VERSION" CASCADE;
SELECT *
INTO :NAME.:"VERSION"
FROM tmp;

DROP VIEW IF EXISTS :NAME.latest;
CREATE VIEW :NAME.latest AS (
    SELECT :'VERSION' as v, * 
    FROM :NAME.:"VERSION"
);