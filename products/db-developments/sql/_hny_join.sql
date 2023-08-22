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
DROP TABLE IF EXISTS HNY_DEVDB_LOOKUP;
WITH
-- Find cases of many-hny-to-one-devdb, after having filtered to highest priority.
-- refer to sql/_hny_match.sql for matching priority logics
MANY_DEVELOPMENTS AS (
    SELECT HNY_ID
    FROM HNY_MATCHES
    GROUP BY HNY_ID
    HAVING COUNT(*) > 1
),

-- Find cases of many-devdb-to-one-hny, after having filtered to highest priority
-- refer to sql/_hny_match.sql for matching priority logics
MANY_HNY AS (
    SELECT A.JOB_NUMBER
    FROM HNY_MATCHES AS A
    GROUP BY A.JOB_NUMBER
    HAVING COUNT(*) > 1
),

-- Add relationship flags, where '1' in both flags means a many-to-many relationship
RELATEFLAGS_HNY_MATCHES AS (
    SELECT
        M.*,
        (CASE
            WHEN HNY_ID IN (SELECT DISTINCT HNY_ID FROM MANY_DEVELOPMENTS) THEN 1
            ELSE 0
        END) AS ONE_HNY_TO_MANY_DEV,
        (CASE
            WHEN JOB_NUMBER IN (SELECT DISTINCT JOB_NUMBER FROM MANY_HNY) THEN 1
            ELSE 0
        END) AS ONE_DEV_TO_MANY_HNY
    FROM HNY_MATCHES AS M
),

-- 5) ASSIGN MATCHES
-- a) for one dev to one hny record simply join on hny_id from the hny_geo table for attributes
ONE_TO_ONE AS (
    SELECT
        R.HNY_ID,
        R.JOB_NUMBER::TEXT AS JOB_NUMBER,
        R.ALL_COUNTED_UNITS::TEXT AS CLASSA_HNYAFF,
        R.TOTAL_UNITS::TEXT AS ALL_HNY_UNITS,
        R.ONE_DEV_TO_MANY_HNY,
        R.ONE_HNY_TO_MANY_DEV,
        H.PROJECT_START_DATE AS PROJECT_START_DATE,
        H.PROJECT_COMPLETION_DATE AS PROJECT_COMPLETION_DATE,
        H.EXTREMELY_LOW_INCOME_UNITS::NUMERIC AS EXTREMELY_LOW_INCOME_UNITS,
        H.VERY_LOW_INCOME_UNITS::NUMERIC AS VERY_LOW_INCOME_UNITS,
        H.LOW_INCOME_UNITS::NUMERIC AS LOW_INCOME_UNITS,
        H.MODERATE_INCOME_UNITS::NUMERIC AS MODERATE_INCOME_UNITS,
        H.MIDDLE_INCOME_UNITS::NUMERIC AS MIDDLE_INCOME_UNITS,
        H.OTHER_INCOME_UNITS::NUMERIC AS OTHER_INCOME_UNITS,
        H.STUDIO_UNITS::NUMERIC AS STUDIO_UNITS,
        H."1_br_units"::NUMERIC AS "1_br_units",
        H."2_br_units"::NUMERIC AS "2_br_units",
        H."3_br_units"::NUMERIC AS "3_br_units",
        H."4_br_units"::NUMERIC AS "4_br_units",
        H."5_br_units"::NUMERIC AS "5_br_units",
        H."6_br+_units"::NUMERIC AS "6_br+_units",
        H.UNKNOWN_BR_UNITS::NUMERIC AS UNKNOWN_BR_UNITS,
        H.COUNTED_RENTAL_UNITS::NUMERIC AS COUNTED_RENTAL_UNITS,
        H.COUNTED_HOMEOWNERSHIP_UNITS::NUMERIC AS COUNTED_HOMEOWNERSHIP_UNITS
    FROM RELATEFLAGS_HNY_MATCHES AS R
    LEFT JOIN HNY_GEO AS H
        ON R.HNY_ID = H.HNY_ID
    WHERE R.ONE_DEV_TO_MANY_HNY = 0 AND R.ONE_HNY_TO_MANY_DEV = 0
),

-- b) For one dev to many hny, group by job_number and sum unit fields, take the min for date fields.
ONE_TO_MANY AS (
    SELECT
        R.JOB_NUMBER,
        SUM(COALESCE(R.ALL_COUNTED_UNITS::INT, '0'))::TEXT AS CLASSA_HNYAFF,
        SUM(COALESCE(R.TOTAL_UNITS::INT, '0'))::TEXT AS ALL_HNY_UNITS,
        R.ONE_DEV_TO_MANY_HNY,
        R.ONE_HNY_TO_MANY_DEV,
        STRING_AGG(R.HNY_ID, '; ') AS HNY_ID,
        MIN(H.PROJECT_START_DATE) AS PROJECT_START_DATE,
        MIN(H.PROJECT_COMPLETION_DATE) AS PROJECT_COMPLETION_DATE,
        SUM(H.EXTREMELY_LOW_INCOME_UNITS::NUMERIC) AS EXTREMELY_LOW_INCOME_UNITS,
        SUM(H.VERY_LOW_INCOME_UNITS::NUMERIC) AS VERY_LOW_INCOME_UNITS,
        SUM(H.LOW_INCOME_UNITS::NUMERIC) AS LOW_INCOME_UNITS,
        SUM(H.MODERATE_INCOME_UNITS::NUMERIC) AS MODERATE_INCOME_UNITS,
        SUM(H.MIDDLE_INCOME_UNITS::NUMERIC) AS MIDDLE_INCOME_UNITS,
        SUM(H.OTHER_INCOME_UNITS::NUMERIC) AS OTHER_INCOME_UNITS,
        SUM(H.STUDIO_UNITS::NUMERIC) AS STUDIO_UNITS,
        SUM(H."1_br_units"::NUMERIC) AS "1_br_units",
        SUM(H."2_br_units"::NUMERIC) AS "2_br_units",
        SUM(H."3_br_units"::NUMERIC) AS "3_br_units",
        SUM(H."4_br_units"::NUMERIC) AS "4_br_units",
        SUM(H."5_br_units"::NUMERIC) AS "5_br_units",
        SUM(H."6_br+_units"::NUMERIC) AS "6_br+_units",
        SUM(H.UNKNOWN_BR_UNITS::NUMERIC) AS UNKNOWN_BR_UNITS,
        SUM(H.COUNTED_RENTAL_UNITS::NUMERIC) AS COUNTED_RENTAL_UNITS,
        SUM(H.COUNTED_HOMEOWNERSHIP_UNITS::NUMERIC) AS COUNTED_HOMEOWNERSHIP_UNITS

    FROM RELATEFLAGS_HNY_MATCHES AS R
    LEFT JOIN HNY_GEO AS H
        ON R.HNY_ID = H.HNY_ID
    WHERE R.ONE_DEV_TO_MANY_HNY = 1 AND R.ONE_HNY_TO_MANY_DEV = 0
    GROUP BY R.JOB_NUMBER, R.ONE_DEV_TO_MANY_HNY, R.ONE_HNY_TO_MANY_DEV
),

-- c) For one hny to many devdb record, pick the devdb record with the least job_number. 
-- Other HNY attributes is set to either max or min but they should be from the same hny record
MANY_TO_ONE AS (
    SELECT
        R.HNY_ID AS HNY_ID,
        MAX(COALESCE(R.ALL_COUNTED_UNITS::INT, '0'))::TEXT AS CLASSA_HNYAFF,
        MAX(COALESCE(R.TOTAL_UNITS::INT, '0'))::TEXT AS ALL_HNY_UNITS,
        R.ONE_DEV_TO_MANY_HNY,
        R.ONE_HNY_TO_MANY_DEV,
        MIN(R.JOB_NUMBER) AS JOB_NUMBER,
        MIN(H.PROJECT_START_DATE) AS PROJECT_START_DATE,
        MIN(H.PROJECT_COMPLETION_DATE) AS PROJECT_COMPLETION_DATE,
        MAX(H.EXTREMELY_LOW_INCOME_UNITS::NUMERIC) AS EXTREMELY_LOW_INCOME_UNITS,
        MAX(H.VERY_LOW_INCOME_UNITS::NUMERIC) AS VERY_LOW_INCOME_UNITS,
        MAX(H.LOW_INCOME_UNITS::NUMERIC) AS LOW_INCOME_UNITS,
        MAX(H.MODERATE_INCOME_UNITS::NUMERIC) AS MODERATE_INCOME_UNITS,
        MAX(H.MIDDLE_INCOME_UNITS::NUMERIC) AS MIDDLE_INCOME_UNITS,
        MAX(H.OTHER_INCOME_UNITS::NUMERIC) AS OTHER_INCOME_UNITS,
        MAX(H.STUDIO_UNITS::NUMERIC) AS STUDIO_UNITS,
        MAX(H."1_br_units"::NUMERIC) AS "1_br_units",
        MAX(H."2_br_units"::NUMERIC) AS "2_br_units",
        MAX(H."3_br_units"::NUMERIC) AS "3_br_units",
        MAX(H."4_br_units"::NUMERIC) AS "4_br_units",
        MAX(H."5_br_units"::NUMERIC) AS "5_br_units",
        MAX(H."6_br+_units"::NUMERIC) AS "6_br+_units",
        MAX(H.UNKNOWN_BR_UNITS::NUMERIC) AS UNKNOWN_BR_UNITS,
        MAX(H.COUNTED_RENTAL_UNITS::NUMERIC) AS COUNTED_RENTAL_UNITS,
        MAX(H.COUNTED_HOMEOWNERSHIP_UNITS::NUMERIC) AS COUNTED_HOMEOWNERSHIP_UNITS

    FROM RELATEFLAGS_HNY_MATCHES AS R
    LEFT JOIN HNY_GEO AS H
        ON R.HNY_ID = H.HNY_ID
    WHERE R.ONE_DEV_TO_MANY_HNY = 0 AND R.ONE_HNY_TO_MANY_DEV = 1
    GROUP BY R.HNY_ID, R.ONE_DEV_TO_MANY_HNY, R.ONE_HNY_TO_MANY_DEV
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
_MANY_TO_MANY AS (
    SELECT
        R.JOB_NUMBER,
        SUM(COALESCE(R.ALL_COUNTED_UNITS::INT, '0'))::TEXT AS CLASSA_HNYAFF,
        SUM(COALESCE(R.TOTAL_UNITS::INT, '0'))::TEXT AS ALL_HNY_UNITS,
        R.ONE_DEV_TO_MANY_HNY,
        R.ONE_HNY_TO_MANY_DEV,
        STRING_AGG(R.HNY_ID, '; ' ORDER BY R.HNY_ID ASC) AS HNY_ID,
        MIN(H.PROJECT_START_DATE) AS PROJECT_START_DATE,
        MIN(H.PROJECT_COMPLETION_DATE) AS PROJECT_COMPLETION_DATE,
        SUM(H.EXTREMELY_LOW_INCOME_UNITS::NUMERIC) AS EXTREMELY_LOW_INCOME_UNITS,
        SUM(H.VERY_LOW_INCOME_UNITS::NUMERIC) AS VERY_LOW_INCOME_UNITS,
        SUM(H.LOW_INCOME_UNITS::NUMERIC) AS LOW_INCOME_UNITS,
        SUM(H.MODERATE_INCOME_UNITS::NUMERIC) AS MODERATE_INCOME_UNITS,
        SUM(H.MIDDLE_INCOME_UNITS::NUMERIC) AS MIDDLE_INCOME_UNITS,
        SUM(H.OTHER_INCOME_UNITS::NUMERIC) AS OTHER_INCOME_UNITS,
        SUM(H.STUDIO_UNITS::NUMERIC) AS STUDIO_UNITS,
        SUM(H."1_br_units"::NUMERIC) AS "1_br_units",
        SUM(H."2_br_units"::NUMERIC) AS "2_br_units",
        SUM(H."3_br_units"::NUMERIC) AS "3_br_units",
        SUM(H."4_br_units"::NUMERIC) AS "4_br_units",
        SUM(H."5_br_units"::NUMERIC) AS "5_br_units",
        SUM(H."6_br+_units"::NUMERIC) AS "6_br+_units",
        SUM(H.UNKNOWN_BR_UNITS::NUMERIC) AS UNKNOWN_BR_UNITS,
        SUM(H.COUNTED_RENTAL_UNITS::NUMERIC) AS COUNTED_RENTAL_UNITS,
        SUM(H.COUNTED_HOMEOWNERSHIP_UNITS::NUMERIC) AS COUNTED_HOMEOWNERSHIP_UNITS

    FROM RELATEFLAGS_HNY_MATCHES AS R
    LEFT JOIN HNY_GEO AS H
        ON R.HNY_ID = H.HNY_ID
    WHERE R.ONE_DEV_TO_MANY_HNY = 1 AND R.ONE_HNY_TO_MANY_DEV = 1
    GROUP BY R.JOB_NUMBER, R.ONE_DEV_TO_MANY_HNY, R.ONE_HNY_TO_MANY_DEV
),

MANY_TO_MANY AS (
    SELECT
        HNY_ID,
        ONE_DEV_TO_MANY_HNY,
        ONE_HNY_TO_MANY_DEV,
        MIN(JOB_NUMBER) AS JOB_NUMBER,
        MAX(CLASSA_HNYAFF) AS CLASSA_HNYAFF,
        MAX(ALL_HNY_UNITS) AS ALL_HNY_UNITS,
        MIN(PROJECT_START_DATE) AS PROJECT_START_DATE,
        MIN(PROJECT_COMPLETION_DATE) AS PROJECT_COMPLETION_DATE,
        MAX(EXTREMELY_LOW_INCOME_UNITS::NUMERIC) AS EXTREMELY_LOW_INCOME_UNITS,
        MAX(VERY_LOW_INCOME_UNITS::NUMERIC) AS VERY_LOW_INCOME_UNITS,
        MAX(LOW_INCOME_UNITS::NUMERIC) AS LOW_INCOME_UNITS,
        MAX(MODERATE_INCOME_UNITS::NUMERIC) AS MODERATE_INCOME_UNITS,
        MAX(MIDDLE_INCOME_UNITS::NUMERIC) AS MIDDLE_INCOME_UNITS,
        MAX(OTHER_INCOME_UNITS::NUMERIC) AS OTHER_INCOME_UNITS,
        MAX(STUDIO_UNITS::NUMERIC) AS STUDIO_UNITS,
        MAX("1_br_units"::NUMERIC) AS "1_br_units",
        MAX("2_br_units"::NUMERIC) AS "2_br_units",
        MAX("3_br_units"::NUMERIC) AS "3_br_units",
        MAX("4_br_units"::NUMERIC) AS "4_br_units",
        MAX("5_br_units"::NUMERIC) AS "5_br_units",
        MAX("6_br+_units"::NUMERIC) AS "6_br+_units",
        MAX(UNKNOWN_BR_UNITS::NUMERIC) AS UNKNOWN_BR_UNITS,
        MAX(COUNTED_RENTAL_UNITS::NUMERIC) AS COUNTED_RENTAL_UNITS,
        MAX(COUNTED_HOMEOWNERSHIP_UNITS::NUMERIC) AS COUNTED_HOMEOWNERSHIP_UNITS
    FROM _MANY_TO_MANY
    GROUP BY HNY_ID, ONE_DEV_TO_MANY_HNY, ONE_HNY_TO_MANY_DEV
)

-- 6) Insert into HNY_devdb_lookup  
SELECT *
INTO
HNY_DEVDB_LOOKUP
FROM (
    SELECT * FROM ONE_TO_ONE
    UNION
    SELECT * FROM ONE_TO_MANY
    UNION
    SELECT * FROM MANY_TO_ONE
    UNION
    SELECT * FROM MANY_TO_MANY
) AS ALL_HNY;
