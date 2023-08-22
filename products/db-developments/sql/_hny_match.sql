/*
DESCRIPTION:
    Merging devdb with hny. This requires the following procedure.

    1) Merge hny data from hpd_units_by_building with hpd_geocode_results,
        and filter to new construction that isn't confidential. Create a unique ID
        using a hash.
    2) Find matches between geocoded devdb and hny using three different methods:
        a) JOIN KEY: geo_bin, geo_bbl
        b) JOIN KEY: geo_bbl
        c) SPATIAL JOIN: geom
        For all three, hny.units_total must be within 5 units dev_db.classa_prop.
        The devdb record cannot be a demolition.
    3) Combine unique matched found by the three methods into the table all_matches,
        assigning priority by match method and development type.
        In cases where a hny record matches with either multiple developments records
        or with a single developments record in multiple ways, matches get assigned
        based on this hierarchy:
            1: Residential new building matched on both BIN & BBL
            2: Residential new building matched only on BBL
            3: Residential new building matched spatially
            4: Alteration or non-residential non-demolition matched on both BIN & BBL
            5: Alteration or non-residential non-demolition matched only on BBL
            6: Alteration or non-residential non-demolition matched spatially
    4) Create HNY_matches:
        For each hny_id, find the highest-priority match(es). This will either be the best
        match, or multiple matches at the same priority-level. Add/remove matches using
        CORR_hny_matches.
    5) Assign flags to indicate one_hny_to_many_dev and/or one_dev_to_many_hny.
    6) Resolve the one-to-many, many-to-one, and many-to-many cases in HNY_matches
        in order to create HNY_lookup
        a) One-to-one matches get assigned directly
        b) For one devdb to many hny, sum the total_units and all_counted_units for all hny rows
        c) For multiple devdb to one hny, assign units to the one with the lowest job_number.
            Remaining matches are retained, but get NULLs in the unit fields.
    7) Merge  devdb with HNY_lookup
        JOIN KEY: job_number


INPUTS:
    hpd_units_by_building (
        ogc_fid,
        project_id,
        project_name,
        number,
        street,
        reporting_construction_type,
        all_counted_units,
        total_units
    )

    hpd_geocode_results (
        * uid,
        geo_bin,
        geo_bbl,
        geo_latitude,
        geo_longitude

    )

    MID_devdb (
        * job_number,
        job_status
        occ_initial,
        occ_proposed,
        resid_flag,
        classa_prop,
        geo_bin,
        geo_bbl,
        ...
    )

    CORR_hny_matches (
        job_number text,
		hny_id text,
		hny_project_id text,
		action text)

OUTPUTS:
    HNY_matches (
        hny_id,
        job_number,
        match_priority,
        job_type,
        resid_flag,
        all_counted_units,
		total_units
    ),

    DevDB_hny_lookup (
        * job_number,
        hny_id,
        classa_hnyaff,
		all_hny_units,
        hny_jobrelate
        ...
    )

IN PREVIOUS VERSION:
    hny_create.sql
    hny_id.sql
    hny_job_lookup.sql
    hny_res_nb_match.sql
    hny_a1_nonres_match.sql
    hny_manual_geomerge.sql
    hny_manual_match.sql
    hny_job_relate.sql
    hny_many_to_many.sql
    hny_dob_match.sql
    dob_hny_id.sql
    dob_affordable_units.sql
*/

CREATE TABLE IF NOT EXISTS CORR_HNY_MATCHES (
    HNY_ID text,
    JOB_NUMBER text,
    HNY_PROJECT_ID text,
    ACTION text
);

DROP TABLE IF EXISTS HNY_GEO;
-- 1) Merge with geocoding results and create a unique ID
WITH HNY AS (
    SELECT
        A.*,
        A.PROJECT_ID AS HNY_PROJECT_ID,
        B.GEO_BBL,
        B.GEO_BIN,
        B.GEO_LATITUDE,
        B.GEO_LONGITUDE,
        A.PROJECT_ID || '/' || COALESCE(LPAD(A.BUILDING_ID, 6, '0'), '') AS HNY_ID,
        (CASE
            WHEN
                B.GEO_LONGITUDE IS NOT NULL
                AND B.GEO_LATITUDE IS NOT NULL
                THEN ST_SETSRID(ST_MAKEPOINT(
                    B.GEO_LONGITUDE::numeric,
                    B.GEO_LATITUDE::numeric
                ), 4326)
        END) AS GEOM
    FROM HPD_UNITS_BY_BUILDING AS A
    INNER JOIN HPD_GEOCODE_RESULTS AS B
        ON A.OGC_FID::text = B.UID
    WHERE
        A.REPORTING_CONSTRUCTION_TYPE = 'New Construction'
        AND A.PROJECT_NAME != 'CONFIDENTIAL'
)

SELECT *
INTO HNY_GEO
FROM HNY;

DROP TABLE IF EXISTS HNY_MATCHES;
WITH
-- 2) Find matches using the three different methods

-- a) Find all matches on both BIN and BBL
BIN_BBL_MATCH AS (
    SELECT
        H.HNY_ID,
        H.HNY_PROJECT_ID,
        D.JOB_NUMBER,
        D.JOB_TYPE,
        D.RESID_FLAG,
        H.TOTAL_UNITS,
        H.ALL_COUNTED_UNITS,
        'BINandBBL' AS MATCH_METHOD
    FROM HNY_GEO AS H
    INNER JOIN MID_DEVDB AS D
        ON
            H.GEO_BBL = D.GEO_BBL
            AND H.GEO_BIN = D.GEO_BIN
            AND ABS(H.TOTAL_UNITS::numeric - D.CLASSA_PROP::numeric) <= 5
            AND H.GEO_BIN IS NOT NULL
            AND H.GEO_BBL IS NOT NULL
            AND D.JOB_STATUS != '9. Withdrawn'
            AND D.JOB_TYPE != 'Demolition'
),

-- b) Find all matches on BBL, but where BIN does not match
BBL_MATCH AS (
    SELECT
        H.HNY_ID,
        H.HNY_PROJECT_ID,
        D.JOB_NUMBER,
        D.JOB_TYPE,
        D.RESID_FLAG,
        H.TOTAL_UNITS,
        H.ALL_COUNTED_UNITS,
        'BBLONLY' AS MATCH_METHOD
    FROM HNY_GEO AS H
    INNER JOIN MID_DEVDB AS D
        ON
            H.GEO_BBL = D.GEO_BBL
            AND (H.GEO_BIN != D.GEO_BIN OR H.GEO_BIN IS NULL OR D.GEO_BIN IS NULL)
            AND ABS(H.TOTAL_UNITS::numeric - D.CLASSA_PROP::numeric) <= 5
            AND H.GEO_BBL IS NOT NULL
            AND D.JOB_STATUS != '9. Withdrawn'
            AND D.JOB_TYPE != 'Demolition'
),

-- c) Find spatial matches where BIN and BBL don't match
SPATIAL_MATCH AS (
    SELECT
        H.HNY_ID,
        H.HNY_PROJECT_ID,
        D.JOB_NUMBER,
        D.JOB_TYPE,
        D.RESID_FLAG,
        H.TOTAL_UNITS,
        H.ALL_COUNTED_UNITS,
        'Spatial' AS MATCH_METHOD
    FROM HNY_GEO AS H
    INNER JOIN MID_DEVDB AS D
        ON
            ST_DWITHIN(H.GEOM::geography, D.GEOM::geography, 5)
            AND (H.GEO_BBL != D.GEO_BBL OR H.GEO_BBL IS NULL OR D.GEO_BBL IS NULL)
            AND (H.GEO_BIN != D.GEO_BIN OR H.GEO_BIN IS NULL OR D.GEO_BIN IS NULL)
            AND ABS(H.TOTAL_UNITS::numeric - D.CLASSA_PROP::numeric) <= 5
            AND H.GEOM IS NOT NULL AND D.GEOM IS NOT NULL
            AND D.JOB_STATUS != '9. Withdrawn'
            AND D.JOB_TYPE != 'Demolition'
),

-- 3) Combine matches into a table of all_matches. Assign match priorities.
ALL_MATCHES AS (
    SELECT
        A.*,
        (
            CASE
                WHEN (
                    JOB_TYPE = 'New Building'
                    AND RESID_FLAG = 'Residential'
                )
                    THEN (CASE
                        WHEN MATCH_METHOD = 'BINandBBL' THEN 1
                        WHEN MATCH_METHOD = 'BBLONLY' THEN 2
                        WHEN MATCH_METHOD = 'Spatial' THEN 3
                    END)
                WHEN (
                    JOB_TYPE = 'Alteration'
                    OR RESID_FLAG != 'Residential'
                )
                    THEN (CASE
                        WHEN MATCH_METHOD = 'BINandBBL' THEN 4
                        WHEN MATCH_METHOD = 'BBLONLY' THEN 5
                        WHEN MATCH_METHOD = 'Spatial' THEN 6
                    END)
            END
        ) AS MATCH_PRIORITY
    FROM (
        SELECT * FROM BIN_BBL_MATCH
        UNION
        SELECT * FROM BBL_MATCH
        UNION
        SELECT * FROM SPATIAL_MATCH
    ) AS A
),

-- 4) Find the highest-priority match(es) and apply corrections
-- First find highest priority match(es) for each hny_id
BEST_MATCHES_BY_HNY AS (
    SELECT
        T1.HNY_ID,
        T1.HNY_PROJECT_ID,
        T1.MATCH_PRIORITY,
        T2.JOB_NUMBER,
        T2.JOB_TYPE,
        T2.RESID_FLAG,
        T2.TOTAL_UNITS,
        T2.ALL_COUNTED_UNITS
    FROM (
        SELECT
            HNY_ID,
            HNY_PROJECT_ID,
            MIN(MATCH_PRIORITY) AS MATCH_PRIORITY
        FROM ALL_MATCHES
        GROUP BY HNY_ID, HNY_PROJECT_ID
    ) AS T1
    INNER JOIN ALL_MATCHES AS T2
        ON
            T1.HNY_ID = T2.HNY_ID
            AND T1.MATCH_PRIORITY = T2.MATCH_PRIORITY
),

-- Then find highest priority match(es) for each job_number	
-- if a job number is same priority for different hny projects. 
-- it should be considered for all of the hny projects		
BEST_MATCHES AS (
    SELECT
        T2.HNY_ID,
        T2.HNY_PROJECT_ID,
        T1.MATCH_PRIORITY,
        T1.JOB_NUMBER,
        T2.JOB_TYPE,
        T2.RESID_FLAG,
        T2.TOTAL_UNITS,
        T2.ALL_COUNTED_UNITS
    FROM (
        SELECT
            JOB_NUMBER,
            MIN(MATCH_PRIORITY) AS MATCH_PRIORITY
        FROM BEST_MATCHES_BY_HNY
        GROUP BY JOB_NUMBER
    ) AS T1
    INNER JOIN BEST_MATCHES_BY_HNY AS T2
        ON
            T1.JOB_NUMBER = T2.JOB_NUMBER
            AND T1.MATCH_PRIORITY = T2.MATCH_PRIORITY
)

SELECT
    HNY_ID,
    HNY_PROJECT_ID,
    JOB_NUMBER,
    TOTAL_UNITS,
    ALL_COUNTED_UNITS
INTO HNY_MATCHES
FROM BEST_MATCHES;

-- Apply corrections to add or remove matches
DELETE FROM HNY_MATCHES
WHERE
    HNY_ID || JOB_NUMBER
    IN (
        SELECT HNY_ID || JOB_NUMBER
        FROM CORR_HNY_MATCHES
        WHERE ACTION = 'remove'
    )
    AND HNY_ID || JOB_NUMBER
    IN (
        SELECT HNY_ID || JOB_NUMBER
        FROM HNY_MATCHES
    );

INSERT INTO HNY_MATCHES (HNY_ID, HNY_PROJECT_ID, JOB_NUMBER, TOTAL_UNITS, ALL_COUNTED_UNITS)
SELECT
    A.HNY_ID,
    A.HNY_PROJECT_ID,
    A.JOB_NUMBER,
    B.TOTAL_UNITS,
    B.ALL_COUNTED_UNITS
FROM CORR_HNY_MATCHES AS A
INNER JOIN HNY_GEO AS B
    ON A.HNY_ID = B.HNY_ID
WHERE
    A.HNY_ID || A.JOB_NUMBER
    IN (
        SELECT HNY_ID || JOB_NUMBER
        FROM CORR_HNY_MATCHES
        WHERE ACTION = 'add'
    )
    AND A.HNY_ID || A.JOB_NUMBER
    NOT IN (
        SELECT HNY_ID || JOB_NUMBER
        FROM HNY_MATCHES
    );

--- cross grouping of all hny_id and job between manual and other matching methods
WITH ASSOCIATIVE_MATCHES AS (
    SELECT DISTINCT
        A.JOB_NUMBER AS J1,
        B.JOB_NUMBER AS J2,
        A.JOB_NUMBER || B.JOB_NUMBER
    FROM HNY_MATCHES AS A
    FULL JOIN HNY_MATCHES AS B
        ON A.HNY_ID = B.HNY_ID
    WHERE A.JOB_NUMBER != B.JOB_NUMBER
)

INSERT INTO HNY_MATCHES (HNY_ID, HNY_PROJECT_ID, JOB_NUMBER, TOTAL_UNITS, ALL_COUNTED_UNITS)
SELECT
    B.HNY_ID,
    B.HNY_PROJECT_ID,
    A.J1,
    B.TOTAL_UNITS,
    B.ALL_COUNTED_UNITS
FROM ASSOCIATIVE_MATCHES AS A
LEFT JOIN HNY_MATCHES AS B
    ON A.J2 = B.JOB_NUMBER
WHERE
    B.HNY_ID || A.J1
    NOT IN (
        SELECT HNY_ID || JOB_NUMBER
        FROM HNY_MATCHES
    );

-- Output unmatched hny records for manual research
DROP TABLE IF EXISTS HNY_NO_MATCH;
WITH
UNMATCHED AS (
    SELECT * FROM HNY_GEO
    WHERE HNY_ID NOT IN (SELECT DISTINCT HNY_ID FROM HNY_MATCHES)
)

SELECT *
INTO HNY_NO_MATCH
FROM UNMATCHED;

-- 5) Identify relationships between devdb records and hny records
DROP TABLE IF EXISTS DEVDB_HNY_LOOKUP;
WITH
-- Find cases of many-hny-to-one-devdb, after having filtered to highest priority
MANY_DEVELOPMENTS AS (
    SELECT HNY_ID
    FROM HNY_MATCHES
    GROUP BY HNY_ID
    HAVING COUNT(*) > 1
),

-- Find cases of many-devdb-to-one-hny, after having filtered to highest priority
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

-- 6) ASSIGN MATCHES   
-- a) Extract one-to-one matches
ONE_TO_ONE AS (
    SELECT
        JOB_NUMBER,
        HNY_ID,
        ALL_COUNTED_UNITS AS CLASSA_HNYAFF,
        TOTAL_UNITS AS ALL_HNY_UNITS,
        ONE_DEV_TO_MANY_HNY,
        ONE_HNY_TO_MANY_DEV
    FROM RELATEFLAGS_HNY_MATCHES
    WHERE
        ONE_DEV_TO_MANY_HNY = 0
        AND ONE_HNY_TO_MANY_DEV = 0
),

-- b) For one dev to many hny, group by job_number and sum unit fields
ONE_TO_MANY AS (
    SELECT
        JOB_NUMBER,
        'Multiple' AS HNY_ID,
        SUM(COALESCE(ALL_COUNTED_UNITS::int, '0'))::text AS CLASSA_HNYAFF,
        SUM(COALESCE(TOTAL_UNITS::int, '0'))::text AS ALL_HNY_UNITS,
        ONE_DEV_TO_MANY_HNY,
        ONE_HNY_TO_MANY_DEV
    FROM RELATEFLAGS_HNY_MATCHES
    WHERE ONE_DEV_TO_MANY_HNY = 1
    GROUP BY JOB_NUMBER, ONE_DEV_TO_MANY_HNY, ONE_HNY_TO_MANY_DEV
),

-- c) For multiple dev to one hny, assign units to the one with the lowest job_number
-- Find the minimum job_number per hny in RELATEFLAGS_hny_matches
MIN_JOB_NUMBER_PER_HNY AS (
    SELECT
        HNY_ID,
        MIN(JOB_NUMBER) AS JOB_NUMBER
    FROM RELATEFLAGS_HNY_MATCHES
    WHERE ONE_HNY_TO_MANY_DEV = 1
    GROUP BY HNY_ID
),

MANY_TO_ONE AS (
    SELECT
        A.JOB_NUMBER,
        /** hny_id has to be set to "Multiple" for many-to-many cases,
                else it comes from HNY_matches **/
        ONE_DEV_TO_MANY_HNY,
        -- Only populate classa_hnyaff for the minimum job_number per hny record
        ONE_HNY_TO_MANY_DEV,
        -- Only populate all_hny_units for the minimum job_number per hny record
        (CASE WHEN
            ONE_HNY_TO_MANY_DEV = 1
            AND ONE_DEV_TO_MANY_HNY = 1
            THEN 'Multiple'
        ELSE A.HNY_ID END) AS HNY_ID,
        (CASE
            WHEN A.JOB_NUMBER || A.HNY_ID IN (SELECT JOB_NUMBER || HNY_ID FROM MIN_JOB_NUMBER_PER_HNY)
            -- If this is a many-to-many match, get summed classa_hnyaff from one_to_many
                THEN CASE
                    WHEN A.JOB_NUMBER IN (SELECT JOB_NUMBER FROM ONE_TO_MANY)
                        THEN (
                            SELECT CLASSA_HNYAFF
                            FROM ONE_TO_MANY AS B
                            WHERE A.JOB_NUMBER = B.JOB_NUMBER
                        )
                    ELSE A.ALL_COUNTED_UNITS
                END
        END) AS CLASSA_HNYAFF,
        (CASE
            WHEN A.JOB_NUMBER || A.HNY_ID IN (SELECT JOB_NUMBER || HNY_ID FROM MIN_JOB_NUMBER_PER_HNY)
            -- If this is a many-to-many, get summed all_hny_units data from one_to_many
                THEN CASE
                    WHEN A.JOB_NUMBER IN (SELECT JOB_NUMBER FROM ONE_TO_MANY)
                        THEN (
                            SELECT ALL_HNY_UNITS
                            FROM ONE_TO_MANY AS B
                            WHERE A.JOB_NUMBER = B.JOB_NUMBER
                        )
                    ELSE A.TOTAL_UNITS
                END
        END) AS ALL_HNY_UNITS
    FROM RELATEFLAGS_HNY_MATCHES AS A
    WHERE ONE_HNY_TO_MANY_DEV = 1
),

-- Combine into a single look-up table					
HNY_LOOKUP AS (
    SELECT * FROM ONE_TO_ONE
    UNION
    SELECT * FROM ONE_TO_MANY
    -- Many-to-many cases are further resolved in many_to_one table, so don't include
    WHERE JOB_NUMBER || HNY_ID NOT IN (SELECT JOB_NUMBER || HNY_ID FROM MANY_TO_ONE)
    UNION
    SELECT * FROM MANY_TO_ONE
)


-- 7) MERGE WITH devdb  
SELECT
    A.JOB_NUMBER,
    B.HNY_ID,
    B.CLASSA_HNYAFF,
    B.ALL_HNY_UNITS,
    (CASE
        WHEN ONE_DEV_TO_MANY_HNY = 0 AND ONE_HNY_TO_MANY_DEV = 0 THEN 'one-to-one'
        WHEN ONE_DEV_TO_MANY_HNY = 1 AND ONE_HNY_TO_MANY_DEV = 0 THEN 'one-to-many'
        WHEN ONE_DEV_TO_MANY_HNY = 0 AND ONE_HNY_TO_MANY_DEV = 1 THEN 'many-to-one'
        WHEN ONE_DEV_TO_MANY_HNY = 1 AND ONE_HNY_TO_MANY_DEV = 1 THEN 'many-to-many'
    END) AS HNY_JOBRELATE
INTO DEVDB_HNY_LOOKUP
FROM MID_DEVDB AS A
INNER JOIN HNY_LOOKUP AS B
    ON A.JOB_NUMBER = B.JOB_NUMBER;
