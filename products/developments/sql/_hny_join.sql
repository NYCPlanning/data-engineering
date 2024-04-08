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
DROP TABLE IF EXISTS hny_devdb_lookup;
-- Find cases of many-hny-to-one-devdb, after having filtered to highest priority.
-- refer to sql/_hny_match.sql for matching priority logics
WITH many_developments AS (
    SELECT hny_id
    FROM hny_matches
    GROUP BY hny_id
    HAVING count(*) > 1
),

-- Find cases of many-devdb-to-one-hny, after having filtered to highest priority
-- refer to sql/_hny_match.sql for matching priority logics
many_hny AS (
    SELECT a.job_number
    FROM hny_matches AS a
    GROUP BY a.job_number
    HAVING count(*) > 1
),

-- Add relationship flags, where '1' in both flags means a many-to-many relationship
relateflags_hny_matches AS (
    SELECT
        m.*,
        CASE
            WHEN hny_id IN (SELECT DISTINCT hny_id FROM many_developments) THEN 1
            ELSE 0
        END AS one_hny_to_many_dev,
        CASE
            WHEN job_number IN (SELECT DISTINCT job_number FROM many_hny) THEN 1
            ELSE 0
        END AS one_dev_to_many_hny
    FROM hny_matches AS m
),

-- 5) ASSIGN MATCHES
-- a) for one dev to one hny record simply join on hny_id from the hny_geo table for attributes
one_to_one AS (
    SELECT
        r.hny_id,
        r.job_number::text AS job_number,
        r.all_counted_units::text AS classa_hnyaff,
        r.total_units::text AS all_hny_units,
        r.one_dev_to_many_hny,
        r.one_hny_to_many_dev,
        h.project_start_date AS project_start_date,
        h.project_completion_date AS project_completion_date,
        h.extremely_low_income_units::numeric AS extremely_low_income_units,
        h.very_low_income_units::numeric AS very_low_income_units,
        h.low_income_units::numeric AS low_income_units,
        h.moderate_income_units::numeric AS moderate_income_units,
        h.middle_income_units::numeric AS middle_income_units,
        h.other_income_units::numeric AS other_income_units,
        h.studio_units::numeric AS studio_units,
        h."1_br_units"::numeric AS "1_br_units",
        h."2_br_units"::numeric AS "2_br_units",
        h."3_br_units"::numeric AS "3_br_units",
        h."4_br_units"::numeric AS "4_br_units",
        h."5_br_units"::numeric AS "5_br_units",
        h."6_br+_units"::numeric AS "6_br+_units",
        h.unknown_br_units::numeric AS unknown_br_units,
        h.counted_rental_units::numeric AS counted_rental_units,
        h.counted_homeownership_units::numeric AS counted_homeownership_units
    FROM relateflags_hny_matches AS r
    LEFT JOIN hny_geo AS h
        ON r.hny_id = h.hny_id
    WHERE r.one_dev_to_many_hny = 0 AND r.one_hny_to_many_dev = 0
),
-- b) For one dev to many hny, group by job_number and sum unit fields, take the min for date fields.
one_to_many AS (
    SELECT
        string_agg(r.hny_id, '; ') AS hny_id,
        r.job_number,
        sum(coalesce(r.all_counted_units::int, '0'))::text AS classa_hnyaff,
        sum(coalesce(r.total_units::int, '0'))::text AS all_hny_units,
        r.one_dev_to_many_hny,
        r.one_hny_to_many_dev,
        min(h.project_start_date) AS project_start_date,
        min(h.project_completion_date) AS project_completion_date,
        sum(h.extremely_low_income_units::numeric) AS extremely_low_income_units,
        sum(h.very_low_income_units::numeric) AS very_low_income_units,
        sum(h.low_income_units::numeric) AS low_income_units,
        sum(h.moderate_income_units::numeric) AS moderate_income_units,
        sum(h.middle_income_units::numeric) AS middle_income_units,
        sum(h.other_income_units::numeric) AS other_income_units,
        sum(h.studio_units::numeric) AS studio_units,
        sum(h."1_br_units"::numeric) AS "1_br_units",
        sum(h."2_br_units"::numeric) AS "2_br_units",
        sum(h."3_br_units"::numeric) AS "3_br_units",
        sum(h."4_br_units"::numeric) AS "4_br_units",
        sum(h."5_br_units"::numeric) AS "5_br_units",
        sum(h."6_br+_units"::numeric) AS "6_br+_units",
        sum(h.unknown_br_units::numeric) AS unknown_br_units,
        sum(h.counted_rental_units::numeric) AS counted_rental_units,
        sum(h.counted_homeownership_units::numeric) AS counted_homeownership_units

    FROM relateflags_hny_matches AS r
    LEFT JOIN hny_geo AS h
        ON r.hny_id = h.hny_id
    WHERE r.one_dev_to_many_hny = 1 AND r.one_hny_to_many_dev = 0
    GROUP BY r.job_number, r.one_dev_to_many_hny, r.one_hny_to_many_dev
),

-- c) For one hny to many devdb record, pick the devdb record with the least job_number. 
-- Other HNY attributes is set to either max or min but they should be from the same hny record
many_to_one AS (
    SELECT
        r.hny_id AS hny_id,
        min(r.job_number) AS job_number,
        max(coalesce(r.all_counted_units::int, '0'))::text AS classa_hnyaff,
        max(coalesce(r.total_units::int, '0'))::text AS all_hny_units,
        r.one_dev_to_many_hny,
        r.one_hny_to_many_dev,
        min(h.project_start_date) AS project_start_date,
        min(h.project_completion_date) AS project_completion_date,
        max(h.extremely_low_income_units::numeric) AS extremely_low_income_units,
        max(h.very_low_income_units::numeric) AS very_low_income_units,
        max(h.low_income_units::numeric) AS low_income_units,
        max(h.moderate_income_units::numeric) AS moderate_income_units,
        max(h.middle_income_units::numeric) AS middle_income_units,
        max(h.other_income_units::numeric) AS other_income_units,
        max(h.studio_units::numeric) AS studio_units,
        max(h."1_br_units"::numeric) AS "1_br_units",
        max(h."2_br_units"::numeric) AS "2_br_units",
        max(h."3_br_units"::numeric) AS "3_br_units",
        max(h."4_br_units"::numeric) AS "4_br_units",
        max(h."5_br_units"::numeric) AS "5_br_units",
        max(h."6_br+_units"::numeric) AS "6_br+_units",
        max(h.unknown_br_units::numeric) AS unknown_br_units,
        max(h.counted_rental_units::numeric) AS counted_rental_units,
        max(h.counted_homeownership_units::numeric) AS counted_homeownership_units

    FROM relateflags_hny_matches AS r
    LEFT JOIN hny_geo AS h
        ON r.hny_id = h.hny_id
    WHERE r.one_dev_to_many_hny = 0 AND r.one_hny_to_many_dev = 1
    GROUP BY r.hny_id, r.one_dev_to_many_hny, r.one_hny_to_many_dev
),
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
        sum(coalesce(r.all_counted_units::int, '0'))::text AS classa_hnyaff,
        sum(coalesce(r.total_units::int, '0'))::text AS all_hny_units,
        r.one_dev_to_many_hny,
        r.one_hny_to_many_dev,
        min(h.project_start_date) AS project_start_date,
        min(h.project_completion_date) AS project_completion_date,
        sum(h.extremely_low_income_units::numeric) AS extremely_low_income_units,
        sum(h.very_low_income_units::numeric) AS very_low_income_units,
        sum(h.low_income_units::numeric) AS low_income_units,
        sum(h.moderate_income_units::numeric) AS moderate_income_units,
        sum(h.middle_income_units::numeric) AS middle_income_units,
        sum(h.other_income_units::numeric) AS other_income_units,
        sum(h.studio_units::numeric) AS studio_units,
        sum(h."1_br_units"::numeric) AS "1_br_units",
        sum(h."2_br_units"::numeric) AS "2_br_units",
        sum(h."3_br_units"::numeric) AS "3_br_units",
        sum(h."4_br_units"::numeric) AS "4_br_units",
        sum(h."5_br_units"::numeric) AS "5_br_units",
        sum(h."6_br+_units"::numeric) AS "6_br+_units",
        sum(h.unknown_br_units::numeric) AS unknown_br_units,
        sum(h.counted_rental_units::numeric) AS counted_rental_units,
        sum(h.counted_homeownership_units::numeric) AS counted_homeownership_units

    FROM relateflags_hny_matches AS r
    LEFT JOIN hny_geo AS h
        ON r.hny_id = h.hny_id
    WHERE r.one_dev_to_many_hny = 1 AND r.one_hny_to_many_dev = 1
    GROUP BY r.job_number, r.one_dev_to_many_hny, r.one_hny_to_many_dev
),
many_to_many AS (
    SELECT
        hny_id,
        min(job_number) AS job_number,
        max(classa_hnyaff) AS classa_hnyaff,
        max(all_hny_units) AS all_hny_units,
        one_dev_to_many_hny,
        one_hny_to_many_dev,
        min(project_start_date) AS project_start_date,
        min(project_completion_date) AS project_completion_date,
        max(extremely_low_income_units::numeric) AS extremely_low_income_units,
        max(very_low_income_units::numeric) AS very_low_income_units,
        max(low_income_units::numeric) AS low_income_units,
        max(moderate_income_units::numeric) AS moderate_income_units,
        max(middle_income_units::numeric) AS middle_income_units,
        max(other_income_units::numeric) AS other_income_units,
        max(studio_units::numeric) AS studio_units,
        max("1_br_units"::numeric) AS "1_br_units",
        max("2_br_units"::numeric) AS "2_br_units",
        max("3_br_units"::numeric) AS "3_br_units",
        max("4_br_units"::numeric) AS "4_br_units",
        max("5_br_units"::numeric) AS "5_br_units",
        max("6_br+_units"::numeric) AS "6_br+_units",
        max(unknown_br_units::numeric) AS unknown_br_units,
        max(counted_rental_units::numeric) AS counted_rental_units,
        max(counted_homeownership_units::numeric) AS counted_homeownership_units
    FROM _many_to_many
    GROUP BY hny_id, one_dev_to_many_hny, one_hny_to_many_dev
)
-- 6) Insert into HNY_devdb_lookup  
SELECT *
INTO hny_devdb_lookup
FROM (
    SELECT * FROM one_to_one
    UNION
    SELECT * FROM one_to_many
    UNION
    SELECT * FROM many_to_one
    UNION
    SELECT * FROM many_to_many
) AS all_hny;
