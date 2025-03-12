/*
DESCRIPTION:
    atypical_roadways is a subset of dcp_lion that 
    satisfies the filter defined in the where statement:

    1. node-level-from (nodelevelf) or node-level-to (nodelevelt): Not ground level (M), not missing level (*), not water level ($)
    2. traffic direction (trafdir) not Pedestrian path: Non-vehicular (P)
    3. feature type (featuretyp) not Railroad (1)
    4. Must be multilane (number_total_lanes)
    5. Bike lane (bikelane) not under: 
        (1) Class I: Separated Greenway
        (2) Class II: Striped Bike Lane
        (4) Links: Connecting segment
        (9) Class II, I: Combination of Class II and I 

    You can find complete data dictionary for dcp_lion at:
    https://www1.nyc.gov/assets/planning/download/pdf/data-maps/open-data/lion_metadata.pdf
    
INPUTS: 
	dcp_lion.latest (
        street, 
        segmentid, 
        streetwidth_min,
        streetwidth_max, 
        lzip, 
        rzip,
        StreetCode, 
        nodelevelf, 
        nodelevelt,
        featuretyp, 
        trafdir, 
        number_total_lanes, 
        bikelane, 
        wkb_geometry geometry(LineString,4326)
    )

OUTPUTS:
	TEMP tmp (
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
    )
*/
CREATE TEMP TABLE tmp AS (
    SELECT 
        street AS streetname, 
        segmentid, 
        streetwidth_min,
        streetwidth_max, 
        lzip AS left_zipcode, 
        rzip AS right_zipcode,
        LEFT(StreetCode, 1) AS borocode, 
        nodelevelf, 
        nodelevelt,
        featuretyp, 
        trafdir, 
        nullif(number_total_lanes, '  ')::NUMERIC AS number_total_lanes, 
        trim(bikelane) AS bikelane, 
        wkb_geometry AS geom
    FROM dcp_lion.latest
    WHERE ((nodelevelf!= 'M' AND nodelevelf!= '*' AND nodelevelf!= '$')
    OR (nodelevelt!= 'M' AND nodelevelt!= '*' AND nodelevelt!= '$'))
    AND trafdir != 'P'
    AND featuretyp != '1'
    AND (nullif(number_total_lanes, '  ')::NUMERIC != 1
    OR nullif(number_total_lanes, '  ')::NUMERIC IS NULL)
    AND trim(bikelane) != '1'
    AND trim(bikelane) != '2'
    AND trim(bikelane) != '4'
    AND trim(bikelane) != '9'
);

\COPY tmp TO PSTDOUT DELIMITER ',' CSV HEADER;