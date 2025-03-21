/*
DESCRIPTION:
    Rename geometry column and export to PSTDOUT for transfer to EDM database
INPUTS: 
	tunnel_ventilation_towers.latest(
        name,
        address,
        link,
        wkb_geometry
    )
OUTPUTS:
	TEMP tmp(name,
        address,
        link,
        geom
    ) >> PSTDOUT
*/

CREATE TEMP TABLE tmp as (
    SELECT 
        name,
        address,
        link,
        wkb_geometry as geom
    FROM tunnel_ventilation_towers.latest
);

\COPY tmp TO PSTDOUT DELIMITER ',' CSV HEADER;