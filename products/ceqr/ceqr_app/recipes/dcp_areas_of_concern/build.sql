/*
DESCRIPTION:
    Rename geometry column and export to PSTDOUT for transfer to EDM database
INPUTS: 
	dcp_areas_of_concern.latest(
        name,
        wkb_geometry
    )
OUTPUTS:
	TEMP tmp(name,
        geom
    ) >> PSTDOUT
*/

CREATE TEMP TABLE tmp as (
    SELECT 
        name,
        wkb_geometry as geom
    FROM dcp_areas_of_concern.latest
);

\COPY tmp TO PSTDOUT DELIMITER ',' CSV HEADER;