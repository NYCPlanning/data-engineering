/*
DESCRIPTION:
    1. Get borough-level high school totals 

INPUT:
    sca_e_projections.:"VERSION" (
        district character varying,
        data_type character varying,
        year character varying,
        pk character varying,
        k character varying,
        grade_1 character varying,
        grade_2 character varying,
        grade_3 character varying,
        grade_4 character varying,
        grade_5 character varying,
        grade_6 character varying,
        grade_7 character varying,
        grade_8 character varying,
        grade_9 character varying,
        grade_10 character varying,
        grade_11 character varying,
        grade_12 character varying,
        ged character varying,
        total character varying
    )

OUTPUT:

    TEMP tmp (
        year text,
        borough character varying,
        hs bigint
    )

*/
CREATE TEMP TABLE tmp as (
    SELECT 
	    LEFT(year, 4) as year,
	    REPLACE(borough_or_district, ' HS', '') as borough,
	    (
            REPLACE(grade_9, ',', '')::integer + 
            REPLACE(grade_10, ',', '')::integer + 
            REPLACE(grade_11, ',', '')::integer + 
            REPLACE(grade_12, ',', '')::integer
        ) as hs
    FROM sca_e_projections.latest
    WHERE borough_or_district ~* 'HS'
    ORDER BY year, borough
);

\COPY tmp TO 'output/_sca_e_projections_by_boro.csv' DELIMITER ',' CSV HEADER;