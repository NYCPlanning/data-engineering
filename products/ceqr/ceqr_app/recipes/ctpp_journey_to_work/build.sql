CREATE TEMP TABLE tmp as (
    SELECT 
        res_tract AS residential_geoid, 
        work_tract AS work_geoid, 
        (CASE 
            WHEN mode = '1' THEN 'Total'
            WHEN mode = '2' THEN 'Car, truck, or van -- Drove alone'
            WHEN mode ='3' THEN 'Car, truck, or van -- In a 2-person carpool'
            WHEN mode ='4' THEN 'Car, truck, or van -- In a 3-person carpool'
            WHEN mode ='5' THEN 'Car, truck, or van -- In a 4-person carpool'
            WHEN mode ='6' THEN 'Car, truck, or van -- In a 5-or-6-person carpool'
            WHEN mode ='7' THEN 'Car, truck, or van -- In a 7-or-more-person carpool'
            WHEN mode ='8' THEN 'Bus or trolley bus'
            WHEN mode ='9' THEN 'Streetcar or trolley car'
            WHEN mode ='10' THEN 'Subway or elevated'
            WHEN mode ='11' THEN 'Railroad'
            WHEN mode ='12' THEN 'Ferryboat'
            WHEN mode ='13' THEN 'Bicycle'
            WHEN mode ='14' THEN 'Walked'
            WHEN mode ='15' THEN 'Taxicab'
            WHEN mode ='16' THEN 'Motorcycle'
            WHEN mode ='17' THEN 'Other method'
            WHEN mode ='18' THEN 'Worked at home'
        END) AS mode,
        "totwork_16+" AS count, 
        standard_error
    FROM ctpp_journey_to_work."2019/09/16"
    WHERE workplace_state_county in (
        '09001', '09005', '09009', '34003', '34013', 
        '34017', '34019', '34021', '34023', '34025', 
        '34027', '34029', '34031', '34035', '34037', 
        '34039', '34041', '36005', '36027', '36047', 
        '36059', '36061', '36071', '36079', '36081', 
        '36085', '36087', '36103', '36105', '36111', 
        '36119')
);
\COPY tmp TO PSTDOUT DELIMITER ',' CSV HEADER;

