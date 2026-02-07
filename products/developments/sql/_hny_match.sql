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
        hny_corrections.
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

    hny_corrections (
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

CREATE TABLE IF NOT EXISTS hny_corrections (
    hny_id text,
    job_number text,
    hny_project_id text,
    action text
);

DROP TABLE IF EXISTS hny_geo;
-- 1) Merge with geocoding results and create a unique ID
WITH hny AS (
    SELECT
        concat_ws(
            '/',
            a.project_id,
            CASE
                WHEN a.building_id IS NULL THEN ''
                WHEN char_length(a.building_id) > 6 THEN a.building_id
                ELSE lpad(a.building_id, 6, '0')
            END
        ) AS hny_id,
        a.project_id AS hny_project_id,
        a.*,
        b.geo_bbl,
        b.geo_bin,
        b.geo_latitude,
        b.geo_longitude,
        CASE
            WHEN b.geo_longitude IS NOT NULL AND b.geo_latitude IS NOT NULL
                THEN st_setsrid(st_makepoint(
                    b.geo_longitude::numeric,
                    b.geo_latitude::numeric
                ), 4326)
        END AS geom
    FROM hpd_units_by_building AS a
    INNER JOIN hpd_geocode_results AS b
        ON a.ogc_fid::text = b.uid
    WHERE
        a.reporting_construction_type = 'New Construction'
        AND a.project_name != 'CONFIDENTIAL'
)

SELECT *
INTO hny_geo
FROM hny;

DROP TABLE IF EXISTS hny_matches;
WITH
-- 2) Find matches using the three different methods

-- a) Find all matches on both BIN and BBL
bin_bbl_match AS (
    SELECT
        h.hny_id,
        h.hny_project_id,
        d.job_number,
        d.job_type,
        d.resid_flag,
        h.total_units,
        h.all_counted_units,
        'BINandBBL' AS match_method
    FROM hny_geo AS h
    INNER JOIN mid_devdb AS d
        ON
            h.geo_bbl = d.geo_bbl
            AND h.geo_bin = d.geo_bin
            AND abs(h.total_units::numeric - d.classa_prop::numeric) <= 5
            AND h.geo_bin IS NOT NULL
            AND h.geo_bbl IS NOT NULL
            AND d.job_status != '9. Withdrawn'
            AND d.job_type != 'Demolition'
),

-- b) Find all matches on BBL, but where BIN does not match
bbl_match AS (
    SELECT
        h.hny_id,
        h.hny_project_id,
        d.job_number,
        d.job_type,
        d.resid_flag,
        h.total_units,
        h.all_counted_units,
        'BBLONLY' AS match_method
    FROM hny_geo AS h
    INNER JOIN mid_devdb AS d
        ON
            h.geo_bbl = d.geo_bbl
            AND (h.geo_bin != d.geo_bin OR h.geo_bin IS NULL OR d.geo_bin IS NULL)
            AND abs(h.total_units::numeric - d.classa_prop::numeric) <= 5
            AND h.geo_bbl IS NOT NULL
            AND d.job_status != '9. Withdrawn'
            AND d.job_type != 'Demolition'
),

-- c) Find spatial matches where BIN and BBL don't match
spatial_match AS (
    SELECT
        h.hny_id,
        h.hny_project_id,
        d.job_number,
        d.job_type,
        d.resid_flag,
        h.total_units,
        h.all_counted_units,
        'Spatial' AS match_method
    FROM hny_geo AS h
    INNER JOIN mid_devdb AS d
        ON
            st_dwithin(h.geom::geography, d.geom::geography, 5)
            AND (h.geo_bbl != d.geo_bbl OR h.geo_bbl IS NULL OR d.geo_bbl IS NULL)
            AND (h.geo_bin != d.geo_bin OR h.geo_bin IS NULL OR d.geo_bin IS NULL)
            AND abs(h.total_units::numeric - d.classa_prop::numeric) <= 5
            AND h.geom IS NOT NULL AND d.geom IS NOT NULL
            AND d.job_status != '9. Withdrawn'
            AND d.job_type != 'Demolition'
),

-- 3) Combine matches into a table of all_matches. Assign match priorities.
all_matches AS (
    SELECT
        a.*,
        CASE
            WHEN job_type = 'New Building' AND resid_flag = 'Residential'
                THEN CASE
                    WHEN match_method = 'BINandBBL' THEN 1
                    WHEN match_method = 'BBLONLY' THEN 2
                    WHEN match_method = 'Spatial' THEN 3
                END
            WHEN job_type = 'Alteration' OR resid_flag != 'Residential'
                THEN CASE
                    WHEN match_method = 'BINandBBL' THEN 4
                    WHEN match_method = 'BBLONLY' THEN 5
                    WHEN match_method = 'Spatial' THEN 6
                END
        END AS match_priority
    FROM (
        SELECT * FROM bin_bbl_match
        UNION
        SELECT * FROM bbl_match
        UNION
        SELECT * FROM spatial_match
    ) AS a
),

-- 4) Find the highest-priority match(es) and apply corrections
-- First find highest priority match(es) for each hny_id
best_matches_by_hny AS (
    SELECT
        t1.hny_id,
        t1.hny_project_id,
        t1.match_priority,
        t2.job_number,
        t2.job_type,
        t2.resid_flag,
        t2.total_units,
        t2.all_counted_units
    FROM (
        SELECT
            hny_id,
            hny_project_id,
            min(match_priority) AS match_priority
        FROM all_matches
        GROUP BY hny_id, hny_project_id
    ) AS t1
    INNER JOIN all_matches AS t2
        ON
            t1.hny_id = t2.hny_id
            AND t1.match_priority = t2.match_priority
),

-- Then find highest priority match(es) for each job_number	
-- if a job number is same priority for different hny projects. 
-- it should be considered for all of the hny projects		
best_matches AS (
    SELECT
        t2.hny_id,
        t2.hny_project_id,
        t1.match_priority,
        t1.job_number,
        t2.job_type,
        t2.resid_flag,
        t2.total_units,
        t2.all_counted_units
    FROM (
        SELECT
            job_number,
            min(match_priority) AS match_priority
        FROM best_matches_by_hny
        GROUP BY job_number
    ) AS t1
    INNER JOIN best_matches_by_hny AS t2
        ON
            t1.job_number = t2.job_number
            AND t1.match_priority = t2.match_priority
)
SELECT
    hny_id,
    hny_project_id,
    job_number,
    total_units,
    all_counted_units
INTO hny_matches
FROM best_matches;

-- Apply corrections to add or remove matches
DELETE FROM hny_matches
WHERE
    hny_id || job_number
    IN (
        SELECT hny_id || job_number
        FROM hny_corrections
        WHERE action = 'remove'
    )
    AND hny_id || job_number
    IN (
        SELECT hny_id || job_number
        FROM hny_matches
    );

INSERT INTO hny_matches (hny_id, hny_project_id, job_number, total_units, all_counted_units)
SELECT
    a.hny_id,
    a.hny_project_id,
    a.job_number,
    b.total_units,
    b.all_counted_units
FROM hny_corrections AS a
INNER JOIN hny_geo AS b
    ON a.hny_id = b.hny_id
WHERE
    a.hny_id || a.job_number
    IN (
        SELECT hny_id || job_number
        FROM hny_corrections
        WHERE action = 'add'
    )
    AND a.hny_id || a.job_number
    NOT IN (
        SELECT hny_id || job_number
        FROM hny_matches
    );

--- cross grouping of all hny_id and job between manual and other matching methods
WITH associative_matches AS (
    SELECT DISTINCT
        a.job_number AS j1,
        b.job_number AS j2
    FROM hny_matches AS a
    FULL JOIN hny_matches AS b
        ON a.hny_id = b.hny_id
    WHERE a.job_number != b.job_number
)
INSERT INTO hny_matches (hny_id, hny_project_id, job_number, total_units, all_counted_units)
SELECT
    b.hny_id,
    b.hny_project_id,
    a.j1,
    b.total_units,
    b.all_counted_units
FROM associative_matches AS a
LEFT JOIN hny_matches AS b
    ON a.j2 = b.job_number
WHERE
    b.hny_id || a.j1
    NOT IN (
        SELECT hny_id || job_number
        FROM hny_matches
    );

-- Output unmatched hny records for manual research
DROP TABLE IF EXISTS hny_no_match;
WITH
unmatched AS (
    SELECT * FROM hny_geo
    WHERE hny_id NOT IN (SELECT DISTINCT hny_id FROM hny_matches)
)
SELECT *
INTO hny_no_match
FROM unmatched;

-- 5) Identify relationships between devdb records and hny records
DROP TABLE IF EXISTS devdb_hny_lookup;
WITH
-- Find cases of many-hny-to-one-devdb, after having filtered to highest priority
many_developments AS (
    SELECT hny_id
    FROM hny_matches
    GROUP BY hny_id
    HAVING count(*) > 1
),

-- Find cases of many-devdb-to-one-hny, after having filtered to highest priority
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

-- 6) ASSIGN MATCHES   
-- a) Extract one-to-one matches
one_to_one AS (
    SELECT
        job_number,
        hny_id,
        all_counted_units AS classa_hnyaff,
        total_units AS all_hny_units,
        one_dev_to_many_hny,
        one_hny_to_many_dev
    FROM relateflags_hny_matches
    WHERE
        one_dev_to_many_hny = 0
        AND one_hny_to_many_dev = 0
),

-- b) For one dev to many hny, group by job_number and sum unit fields
one_to_many AS (
    SELECT
        job_number,
        'Multiple' AS hny_id,
        sum(coalesce(all_counted_units::int, '0'))::text AS classa_hnyaff,
        sum(coalesce(total_units::int, '0'))::text AS all_hny_units,
        one_dev_to_many_hny,
        one_hny_to_many_dev
    FROM relateflags_hny_matches
    WHERE one_dev_to_many_hny = 1
    GROUP BY job_number, one_dev_to_many_hny, one_hny_to_many_dev
),

-- c) For multiple dev to one hny, assign units to the one with the lowest job_number
-- Find the minimum job_number per hny in RELATEFLAGS_hny_matches
min_job_number_per_hny AS (
    SELECT
        min(job_number) AS job_number,
        hny_id
    FROM relateflags_hny_matches
    WHERE one_hny_to_many_dev = 1
    GROUP BY hny_id
),

many_to_one AS (
    SELECT
        a.job_number,
        /** hny_id has to be set to "Multiple" for many-to-many cases,
                else it comes from HNY_matches **/
        CASE WHEN
            one_hny_to_many_dev = 1
            AND one_dev_to_many_hny = 1
            THEN 'Multiple'
        ELSE a.hny_id END AS hny_id,
        -- Only populate classa_hnyaff for the minimum job_number per hny record
        CASE
            WHEN a.job_number || a.hny_id IN (SELECT job_number || hny_id FROM min_job_number_per_hny)
            -- If this is a many-to-many match, get summed classa_hnyaff from one_to_many
                THEN CASE
                    WHEN a.job_number IN (SELECT job_number FROM one_to_many)
                        THEN (
                            SELECT classa_hnyaff
                            FROM one_to_many AS b
                            WHERE a.job_number = b.job_number
                        )
                    ELSE a.all_counted_units
                END
        END AS classa_hnyaff,
        -- Only populate all_hny_units for the minimum job_number per hny record
        CASE
            WHEN a.job_number || a.hny_id IN (SELECT job_number || hny_id FROM min_job_number_per_hny)
            -- If this is a many-to-many, get summed all_hny_units data from one_to_many
                THEN CASE
                    WHEN a.job_number IN (SELECT job_number FROM one_to_many)
                        THEN (
                            SELECT all_hny_units
                            FROM one_to_many AS b
                            WHERE a.job_number = b.job_number
                        )
                    ELSE a.total_units
                END
        END AS all_hny_units,
        one_dev_to_many_hny,
        one_hny_to_many_dev
    FROM relateflags_hny_matches AS a
    WHERE one_hny_to_many_dev = 1
),

-- Combine into a single look-up table					
hny_lookup AS (
    SELECT * FROM one_to_one
    UNION
    SELECT * FROM one_to_many
    -- Many-to-many cases are further resolved in many_to_one table, so don't include
    WHERE job_number || hny_id NOT IN (SELECT job_number || hny_id FROM many_to_one)
    UNION
    SELECT * FROM many_to_one
)

-- 7) MERGE WITH devdb  
SELECT
    a.job_number,
    b.hny_id,
    b.classa_hnyaff,
    b.all_hny_units,
    CASE
        WHEN one_dev_to_many_hny = 0 AND one_hny_to_many_dev = 0 THEN 'one-to-one'
        WHEN one_dev_to_many_hny = 1 AND one_hny_to_many_dev = 0 THEN 'one-to-many'
        WHEN one_dev_to_many_hny = 0 AND one_hny_to_many_dev = 1 THEN 'many-to-one'
        WHEN one_dev_to_many_hny = 1 AND one_hny_to_many_dev = 1 THEN 'many-to-many'
    END AS hny_jobrelate
INTO devdb_hny_lookup
FROM mid_devdb AS a
INNER JOIN hny_lookup AS b
    ON a.job_number = b.job_number;
