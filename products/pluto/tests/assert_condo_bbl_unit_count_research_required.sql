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
				1. Manually research failing records. 
                2. If confirmed, update the corrections file with the correct total and res unit number; 
                not confirmed bbls need to be added to the `ignored_bbls_for_unit_count_test` seed file.
                3. Re-run PLUTO build. If this check passes, share reports from `models/qaqc/reports` with Amanda to send them to DOF.
			'''
        }
    ) 
}}

-- Find records where multiple unit BBLs have coop_apts > 1
WITH condo_prime_bbls AS (
    SELECT primebbl
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

-- Identify bbl offenders that are not actively corrected
uncorrected_primebbl_offenders AS (
    SELECT *
    FROM primebbl_offenders
    WHERE primebbl NOT IN (
        SELECT bbl
        FROM {{ ref('qaqc_int__active_condo_bbl_unitsres_corrections') }}
    )
),

not_ignored_primebbls AS (
    SELECT *
    FROM uncorrected_primebbl_offenders
    WHERE primebbl NOT IN (
        SELECT bbl::decimal::bigint::text
        FROM {{ ref('ignored_bbls_for_unit_count_test') }}
    )
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
    FROM not_ignored_primebbls AS l
    LEFT JOIN {{ ref('qaqc_int__devdb_bbl_units_summary') }} AS r
        ON l.primebbl = r.bbl
)

-- Final selection of records where the unit difference exceeds threshold
SELECT *
FROM offenders_joined_with_devdb
WHERE diff >= 50 OR diff <= -50	-- 50 is an arbitrary threshold
ORDER BY dof_matches_devdb_units
