/*
DESCRIPTION:
    Merging devdb with hny. This requires the following procedure.
    Following _hny.sql query with HNY_matches were completed

    5) Assign flags to indicate one_hny_to_many_dev and/or one_dev_to_many_hny.
        a) handle one devdb to many hny records cases by concatenating those hny_ids and combines units/fields 
        to create a single record
        b) Rest of the cases (one hny to many devdb, many hny to many devdb) are handled by including every 
        possible joins with devdb records for each hny recor.

    6) Combine the two tables to create HNY_devdb_lookup

*/

-- 5) Identify relationships between devdb records and hny records
DROP TABLE IF EXISTS HNY_devdb_lookup;
WITH 
	-- Find cases of many-hny-to-one-devdb, after having filtered to highest priority.
    -- refer to sql/_hny_match.sql for matching priority logics
	many_developments AS (SELECT hny_id
				FROM HNY_matches
				GROUP BY hny_id
                HAVING COUNT(*)>1),
				
	-- Find cases of many-devdb-to-one-hny, after having filtered to highest priority
    -- refer to sql/_hny_match.sql for matching priority logics
	many_hny AS (SELECT a.job_number
				FROM HNY_matches a
				GROUP BY a.job_number
                HAVING COUNT(*)>1),	

	-- Add relationship flags, where '1' in both flags means a many-to-many relationship
    RELATEFLAGS_hny_matches AS
    (SELECT m.*,
		(CASE 
			WHEN hny_id IN (SELECT DISTINCT hny_id FROM many_developments) THEN 1
			ELSE 0 
		END) AS one_hny_to_many_dev,
		(CASE 
			WHEN job_number IN (SELECT DISTINCT job_number FROM many_hny) THEN 1
			ELSE 0
		END) AS one_dev_to_many_hny
    FROM HNY_matches m),

-- 5) ASSIGN MATCHES
    -- a) for one dev to one hny record simply join on hny_id from the hny_geo table for attributes
    one_to_one AS (
        SELECT 
            r.hny_id,
            r.job_number::TEXT as job_number,
            r.all_counted_units::text as classa_hnyaff,
            r.total_units::text as all_hny_units,
            r.one_dev_to_many_hny,
            r.one_hny_to_many_dev,
            h.project_start_date as project_start_date,
            h.project_completion_date as project_completion_date,
            h.extremely_low_income_units::NUMERIC as extremely_low_income_units,
            h.very_low_income_units::NUMERIC as very_low_income_units,
            h.low_income_units::NUMERIC as low_income_units,
            h.moderate_income_units::NUMERIC as moderate_income_units,
            h.middle_income_units::NUMERIC as middle_income_units,
            h.other_income_units::NUMERIC as other_income_units,
            h.studio_units::NUMERIC as studio_units,
            h."1_br_units"::NUMERIC as "1_br_units",
            h."2_br_units"::NUMERIC as "2_br_units",
            h."3_br_units"::NUMERIC as "3_br_units",
            h."4_br_units"::NUMERIC as "4_br_units",
            h."5_br_units"::NUMERIC as "5_br_units",
            h."6_br+_units"::NUMERIC as "6_br+_units",
            h.unknown_br_units::NUMERIC as unknown_br_units,
            h.counted_rental_units::NUMERIC as counted_rental_units,
            h.counted_homeownership_units::NUMERIC as counted_homeownership_units
        FROM RELATEFLAGS_hny_matches r
        LEFT JOIN HNY_geo h
        ON r.hny_id = h.hny_id
        WHERE r.one_dev_to_many_hny = 0 AND r.one_hny_to_many_dev = 0       
    ),
	-- b) For one dev to many hny, group by job_number and sum unit fields, take the min for date fields.
	one_to_many AS (SELECT 
        string_agg(r.hny_id, '; ') AS hny_id,
        r.job_number, 
        SUM(COALESCE(r.all_counted_units::int, '0'))::text AS classa_hnyaff,
        SUM(COALESCE(r.total_units::int, '0'))::text AS all_hny_units,
        r.one_dev_to_many_hny,
        r.one_hny_to_many_dev,
        MIN(h.project_start_date) as project_start_date,
        MIN(h.project_completion_date) as project_completion_date,
        SUM(h.extremely_low_income_units::NUMERIC) as extremely_low_income_units,
        SUM(h.very_low_income_units::NUMERIC) as very_low_income_units,
        SUM(h.low_income_units::NUMERIC) as low_income_units,
        SUM(h.moderate_income_units::NUMERIC) as moderate_income_units,
        SUM(h.middle_income_units::NUMERIC) as middle_income_units,
        SUM(h.other_income_units::NUMERIC) as other_income_units,
        SUM(h.studio_units::NUMERIC) as studio_units,
        SUM(h."1_br_units"::NUMERIC) as "1_br_units",
        SUM(h."2_br_units"::NUMERIC) as "2_br_units",
        SUM(h."3_br_units"::NUMERIC) as "3_br_units",
        SUM(h."4_br_units"::NUMERIC) as "4_br_units",
        SUM(h."5_br_units"::NUMERIC) as "5_br_units",
        SUM(h."6_br+_units"::NUMERIC) as "6_br+_units",
        SUM(h.unknown_br_units::NUMERIC) as unknown_br_units,
        SUM(h.counted_rental_units::NUMERIC) as counted_rental_units,
        SUM(h.counted_homeownership_units::NUMERIC) as counted_homeownership_units
                        
        FROM RELATEFLAGS_hny_matches r
        LEFT JOIN HNY_geo h
        ON r.hny_id = h.hny_id
        WHERE r.one_dev_to_many_hny = 1 AND r.one_hny_to_many_dev = 0
        GROUP BY r.job_number, r.one_dev_to_many_hny, r.one_hny_to_many_dev), 

    -- c) For one hny to many devdb record, pick the devdb record with the least job_number. 
    -- Other HNY attributes is set to either max or min but they should be from the same hny record
	many_to_one AS (SELECT 
        r.hny_id AS hny_id,
        MIN(r.job_number) as job_number, 
        MAX(COALESCE(r.all_counted_units::int, '0'))::text AS classa_hnyaff,
        MAX(COALESCE(r.total_units::int, '0'))::text AS all_hny_units,
        r.one_dev_to_many_hny,
        r.one_hny_to_many_dev,
        MIN(h.project_start_date) as project_start_date,
        MIN(h.project_completion_date) as project_completion_date,
        MAX(h.extremely_low_income_units::NUMERIC) as extremely_low_income_units,
        MAX(h.very_low_income_units::NUMERIC) as very_low_income_units,
        MAX(h.low_income_units::NUMERIC) as low_income_units,
        MAX(h.moderate_income_units::NUMERIC) as moderate_income_units,
        MAX(h.middle_income_units::NUMERIC) as middle_income_units,
        MAX(h.other_income_units::NUMERIC) as other_income_units,
        MAX(h.studio_units::NUMERIC) as studio_units,
        MAX(h."1_br_units"::NUMERIC) as "1_br_units",
        MAX(h."2_br_units"::NUMERIC) as "2_br_units",
        MAX(h."3_br_units"::NUMERIC) as "3_br_units",
        MAX(h."4_br_units"::NUMERIC) as "4_br_units",
        MAX(h."5_br_units"::NUMERIC) as "5_br_units",
        MAX(h."6_br+_units"::NUMERIC) as "6_br+_units",
        MAX(h.unknown_br_units::NUMERIC) as unknown_br_units,
        MAX(h.counted_rental_units::NUMERIC) as counted_rental_units,
        MAX(h.counted_homeownership_units::NUMERIC) as counted_homeownership_units
                        
        FROM RELATEFLAGS_hny_matches r
        LEFT JOIN HNY_geo h
        ON r.hny_id = h.hny_id
        WHERE r.one_dev_to_many_hny = 0 AND r.one_hny_to_many_dev = 1
        GROUP BY r.hny_id, r.one_dev_to_many_hny, r.one_hny_to_many_dev), 
    -- d) many-to-many relationship requires a two-step process to create
        -- 1) first GROUP BY on the job_number to create the array of hny_ids. Since the HNY_matches is created in the
        -- previous steps such that the matches are comprehensive, i.e. every hny record that joins with other devdb
        -- records has a unique record that represents the match even when they are not direcly matches with spatial
        -- joins or bbl joins. For more details see the associative_matches step in the _hny_match.sql, that each array
        -- would have to contain full set of HNY_id cross-matched with different devdb records.
        -- 2) use the newly created hny_id array to GROUP BY to squash the job_number to create one-to-one relationship
        -- The only caveat here is that the sequence of the hny_id in the array would HAVE a impact on this "deduplication"
        -- process and does not guarantee a unique record at the end. 
    _many_to_many AS (
        SELECT 
            string_agg(r.hny_id, '; ' ORDER BY r.hny_id ASC) AS hny_id,
            r.job_number, 
            SUM(COALESCE(r.all_counted_units::int, '0'))::text AS classa_hnyaff,
            SUM(COALESCE(r.total_units::int, '0'))::text AS all_hny_units,
            r.one_dev_to_many_hny,
            r.one_hny_to_many_dev,
            MIN(h.project_start_date) as project_start_date,
            MIN(h.project_completion_date) as project_completion_date,
            SUM(h.extremely_low_income_units::NUMERIC) as extremely_low_income_units,
            SUM(h.very_low_income_units::NUMERIC) as very_low_income_units,
            SUM(h.low_income_units::NUMERIC) as low_income_units,
            SUM(h.moderate_income_units::NUMERIC) as moderate_income_units,
            SUM(h.middle_income_units::NUMERIC) as middle_income_units,
            SUM(h.other_income_units::NUMERIC) as other_income_units,
            SUM(h.studio_units::NUMERIC) as studio_units,
            SUM(h."1_br_units"::NUMERIC) as "1_br_units",
            SUM(h."2_br_units"::NUMERIC) as "2_br_units",
            SUM(h."3_br_units"::NUMERIC) as "3_br_units",
            SUM(h."4_br_units"::NUMERIC) as "4_br_units",
            SUM(h."5_br_units"::NUMERIC) as "5_br_units",
            SUM(h."6_br+_units"::NUMERIC) as "6_br+_units",
            SUM(h.unknown_br_units::NUMERIC) as unknown_br_units,
            SUM(h.counted_rental_units::NUMERIC) as counted_rental_units,
            SUM(h.counted_homeownership_units::NUMERIC) as counted_homeownership_units
                            
        FROM RELATEFLAGS_hny_matches r
        LEFT JOIN HNY_geo h
        ON r.hny_id = h.hny_id
        WHERE r.one_dev_to_many_hny = 1 AND r.one_hny_to_many_dev = 1
        GROUP BY r.job_number, r.one_dev_to_many_hny, r.one_hny_to_many_dev),
    many_to_many as (
        SELECT
            hny_id,
            MIN(job_number) as job_number, 
            MAX(classa_hnyaff) as classa_hnyaff,
            MAX(all_hny_units) as all_hny_units,
            one_dev_to_many_hny,
            one_hny_to_many_dev,
            MIN(project_start_date) as project_start_date,
            MIN(project_completion_date) as project_completion_date,
            MAX(extremely_low_income_units::NUMERIC) as extremely_low_income_units,
            MAX(very_low_income_units::NUMERIC) as very_low_income_units,
            MAX(low_income_units::NUMERIC) as low_income_units,
            MAX(moderate_income_units::NUMERIC) as moderate_income_units,
            MAX(middle_income_units::NUMERIC) as middle_income_units,
            MAX(other_income_units::NUMERIC) as other_income_units,
            MAX(studio_units::NUMERIC) as studio_units,
            MAX("1_br_units"::NUMERIC) as "1_br_units",
            MAX("2_br_units"::NUMERIC) as "2_br_units",
            MAX("3_br_units"::NUMERIC) as "3_br_units",
            MAX("4_br_units"::NUMERIC) as "4_br_units",
            MAX("5_br_units"::NUMERIC) as "5_br_units",
            MAX("6_br+_units"::NUMERIC) as "6_br+_units",
            MAX(unknown_br_units::NUMERIC) as unknown_br_units,
            MAX(counted_rental_units::NUMERIC) as counted_rental_units,
            MAX(counted_homeownership_units::NUMERIC) as counted_homeownership_units
        FROM _many_to_many
        GROUP BY hny_id,  one_dev_to_many_hny, one_hny_to_many_dev
    ) 
-- 6) Insert into HNY_devdb_lookup  
SELECT 
*
INTO 
HNY_devdb_lookup
FROM (
SELECT * FROM one_to_one
UNION
SELECT * FROM one_to_many
UNION
SELECT * FROM many_to_one
UNION
SELECT * FROM many_to_many) all_hny;

