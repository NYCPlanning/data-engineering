{{ 
    config(
        tags = ['de_check'], 
        meta = {
            'description': '''
				This test identifies condo records that likely have incorrect unit counts in DOF PTS data, affecting the final PLUTO table. 
				The first 2 CTEs determine prime BBLs for condos with multiple unit records where their residential unit count != 1 (expected value). 
				The remaining logic cross-checks these BBL "offenders" with the PLUTO corrections file and the Developments database (aka DevDB) to narrow down the records needing manual review.
			''',
            'next_steps': '''
				Manually research failing records. If confirmed, update the corrections file with the correct unit counts and share a report with Amanda to send to DOF.
			'''
        }
    ) 
}}

-- Find condo records where multiple unit BBLs have coop_apts > 1
WITH condo_prime_bbls AS (
    SELECT DISTINCT primebbl
    FROM {{ ref('pluto_rpad_geo') }}
    WHERE coop_apts > 1
    GROUP BY primebbl
    HAVING count(*) > 1
),

primebbl_offenders AS (
    SELECT
        primebbl,
        sum(coop_apts) AS coop_apts,
        sum(units) AS units
    FROM {{ ref('pluto_rpad_geo') }}
    WHERE
        primebbl IN (SELECT primebbl FROM condo_prime_bbls)
        AND tl NOT LIKE '75%'
    GROUP BY primebbl
),

historical_unit_corrections AS (
    SELECT
        bbl,
        old_value::numeric
    FROM {{ source("recipe_sources", "pluto_input_research") }}
    WHERE field = 'unitsres' AND substring(bbl, 7, 2) = '75'
),

current_unit_corrections AS (
    SELECT l.* FROM historical_unit_corrections AS l
    INNER JOIN primebbl_offenders AS r
        ON l.bbl = r.primebbl AND l.old_value = r.coop_apts
),

uncorrected_primebbl_offenders AS (
    SELECT *
    FROM primebbl_offenders
    WHERE primebbl NOT IN (SELECT bbl FROM current_unit_corrections)
),

-- Filter DevDB for uncorrected offenders
devdb_uncorrected_offenders AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY bbl, bin ORDER BY date_complete DESC) AS order_num,
        CASE
            WHEN classa_prop IS null AND job_status = '5. Completed Construction' THEN 0
            ELSE classa_prop
        END AS classa_prop_modified
    FROM {{ ref("stg__dcp_developments") }}
    WHERE
        bbl IN
        (SELECT DISTINCT primebbl FROM uncorrected_primebbl_offenders)
        AND job_status IN ('5. Completed Construction', '4. Partially Completed Construction')
),

devdb_uncorrected_offender_grouped_by_latest_bin AS (
    SELECT
        bbl,
        bin,
        units_co,
        classa_prop_modified AS classa_prop,
        job_type,
        job_status,
        date_complete
    FROM devdb_uncorrected_offenders
    WHERE order_num = 1
),

devdb_uncorrected_offender_grouped_by_bbl AS (
    SELECT
        bbl,
        sum(units_co) AS units_co,
        sum(classa_prop) AS classa_prop,
        count(*) AS count_bins
    FROM devdb_uncorrected_offender_grouped_by_latest_bin
    WHERE job_type <> 'Demolition' -- Exclude demolished buildings as property taxes may still be paid, aligning with the DOF data
    GROUP BY bbl
),

-- Join DevDB with uncorrected offenders
offenders_joined_with_devdb AS (
    SELECT
        l.*,
        r.units_co,
        r.classa_prop,
        r.count_bins,
        l.coop_apts = r.classa_prop AS dof_matches_devdb_units,
        r.classa_prop - l.coop_apts AS diff
    FROM uncorrected_primebbl_offenders AS l
    LEFT JOIN devdb_uncorrected_offender_grouped_by_bbl AS r
        ON l.primebbl = r.bbl
    ORDER BY dof_matches_devdb_units
)

-- Final selection of records where the unit difference exceeds threshold
SELECT *
FROM offenders_joined_with_devdb
WHERE diff >= 50 OR diff <= -50	-- 50 is an arbitrary threshold
