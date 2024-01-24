/*
DESCRIPTION:
    1. Standardize and concatenate the three sources of capacity project data
    2. Assign capital plan year based on data source
    3. Parse school name to determine the organization level
    4. Using the org_level, estimate the proporiton of capacity in each level
    5. Output the full source table to a csv for easy geocoding
INPUTS: 
	sca_capacity_projects_prev.latest(
        district,
        school,
        borough,
        address,
        number_of_seats,
        opening_&_anticipated_opening
    ),
    sca_capacity_projects_current.latest(
        district,
        school,
        borough,
        location,
        capacity,
        anticipated_opening
    ),
    sca_capacity_projects_tcu.latest(
        district,
        school,
        borough,
        location,
        capacity,
        anticipated_opening
    )
OUTPUTS:
	TEMP tmp(uid,
        name,
        org_level,
        district,
        capacity,
        pct_ps,
        pct_is,
        pct_hs,
        guessed_pct,
        start_date,
        capital_plan,
        borough,
        address
    ) >> _sca_capacity_projects.csv
*/

CREATE TEMP TABLE tmp as (
    WITH combined AS
        (SELECT
            district,
            school as name,
            borough,
            address,
            COALESCE(number_of_seats, '0') as capacity,
            TO_DATE("opening_&_anticipated_opening", 'month yyyy') as start_date,
            '15-19' as capital_plan
        FROM sca_capacity_projects_prev.latest
        UNION
        SELECT
            district,
            school as name,
            borough,
            location as address,
            COALESCE(capacity, '0') as capacity,
            TO_DATE(anticipated_opening, 'month yyyy') as start_date,
            '20-24' as capital_plan
        FROM sca_capacity_projects_current.latest
        UNION
        SELECT
            district,
            school as name,
            borough,
            location as address,
            COALESCE(capacity, '0') as capacity,
            ('01-01-'||anticipated_opening)::date as start_date,
            '20-24' as capital_plan
        FROM sca_capacity_projects_tcu.latest),

    org_levels AS
        (SELECT 
            md5(CAST((c.*) AS text)) as uid,
            TRIM(c.name) as name,
            CASE
                WHEN REGEXP_REPLACE(c.name, '[^\w]+','','g') LIKE '%3K%' THEN '3K'
                WHEN REGEXP_REPLACE(c.name, '[^\w]+','','g') LIKE '%PK%' THEN 'PK'
                WHEN REGEXP_REPLACE(c.name, '[^\w]+','','g') LIKE '%PREK%' THEN 'PK'
                WHEN REGEXP_REPLACE(c.name, '[^\w]+','','g') LIKE '%PSIS%' THEN 'PSIS'
                WHEN REGEXP_REPLACE(c.name, '[^\w]+','','g') LIKE '%ISHS%' THEN 'ISHS'
                WHEN REGEXP_REPLACE(c.name, '[^\w]+','','g') LIKE '%PSHS%' THEN 'PSHS'
                WHEN REGEXP_REPLACE(c.name, '[^\w]+','','g') LIKE '%PS%' THEN 'PS'
                WHEN REGEXP_REPLACE(c.name, '[^\w]+','','g') LIKE '%IS%' THEN 'IS'
                WHEN REGEXP_REPLACE(c.name, '[^\w]+','','g') LIKE '%MS%' THEN 'IS'
                WHEN REGEXP_REPLACE(c.name, '[^\w]+','','g') LIKE '%HS%' THEN 'HS'
                WHEN REGEXP_REPLACE(c.name, '[^\w]+','','g') LIKE '%HIGH%' THEN 'HS'
                WHEN REGEXP_REPLACE(c.name, '[^\w]+','','g') LIKE '%D75%' THEN NULL
            END as org_level,
            c.district,
            REPLACE(c.capacity,',','') as capacity,
            CASE
                WHEN TRIM(borough) = 'M' THEN 'Manhattan'
                WHEN TRIM(borough) = 'X' THEN 'Bronx'
                WHEN TRIM(borough) = 'K' THEN 'Brooklyn'
                WHEN TRIM(borough) = 'Q' THEN 'Queens'
                WHEN TRIM(borough) = 'R' THEN 'Staten Island'
            END as borough,
            c.address,
            c.start_date,
            c.capital_plan
        
        FROM combined c)

    SELECT
        a.uid,
        a.name,
        a.org_level,
        a.district,
        a.capacity,
        CASE 
            WHEN org_level = 'PS' THEN 1
            WHEN org_level = 'PSIS' THEN 0.5
            WHEN org_level = 'PSHS' THEN 0.5
            ELSE 0
        END as pct_ps,
        CASE 
            WHEN org_level = 'IS' THEN 1
            WHEN org_level = 'PSIS' THEN 0.5
            WHEN org_level = 'ISHS' THEN 0.5
            ELSE 0
        END as pct_is,
            CASE 
            WHEN org_level = 'HS' THEN 1
            WHEN org_level = 'PSHS' THEN 0.5
            WHEN org_level = 'ISHS' THEN 0.5
            ELSE 0
        END as pct_hs,
        CASE
            WHEN org_level in ('PSIS','PSHS','ISHS') THEN TRUE
            ELSE FALSE
        END as guessed_pct,
        a.start_date,
        a.capital_plan,
        a.borough,
        a.address
            
    FROM org_levels a
);

\COPY tmp TO 'output/_sca_capacity_projects.csv' DELIMITER ',' CSV HEADER;