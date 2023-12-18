/*
DESCRIPTION:
    Read data from PSTDIN and transfer to EDM database
INPUTS: 
	PSTDIN >> 
    TEMP temp(name text,
        address text,
        link text,
        geom geometry
    ) 
OUTPUTS:
	tunnel_ventilation_towers.latest(
        Same schema as tmp
    )
*/

CREATE TEMP TABLE tmp (
    name text,
    address text,
    link text,
    geom geometry(Point,4326)
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