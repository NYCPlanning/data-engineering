-- DCP's Affordable Housing Fair Share tracker (AHFT): one row per NYC Community
-- District (59 total - the five boroughs' full set; no park/airport/cemetery special
-- codes), with housing-production stats and `ahft_rank`, 1 (lowest rate of
-- affordable housing development, e.g. Bay Ridge/BK10) to 59 (highest).
-- The source's `Community District` column (e.g. "BK10") is a 2-letter borough
-- abbreviation + 2-digit district number; `borocd` recombines it into LIFT's own
-- `cd` column format instead (BORO int 1-5 * 100 + district, e.g. Brooklyn CD10 ->
-- 310), so lift_supplemented can join on it directly - see README.
WITH raw AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_housing_ahft') }}
),

final AS (
    SELECT
        "Community District" AS community_district,
        CASE LEFT("Community District", 2)
            WHEN 'MN' THEN 1
            WHEN 'BX' THEN 2
            WHEN 'BK' THEN 3
            WHEN 'QN' THEN 4
            WHEN 'SI' THEN 5
        END * 100 + TRY_CAST(RIGHT("Community District", 2) AS INTEGER) AS borocd,
        -- CAST ... AS VARCHAR first: DuckDB's CSV sniffer infers a numeric type directly
        -- for columns that never contain a comma/percent sign (e.g. AHFT Rank below), so
        -- TRIM/REPLACE need a VARCHAR to work on regardless of what got inferred.
        TRY_CAST(REPLACE(TRIM(CAST("Housing Units (2020 Census)" AS VARCHAR)), ',', '') AS BIGINT)
            AS housing_units_2020_census,
        TRY_CAST(TRIM(CAST("Net New Housing Units 4/1/20 to 6/30/21 (Housing Database)" AS VARCHAR)) AS BIGINT)
            AS net_new_housing_units,
        TRY_CAST(
            REPLACE(
                TRIM(CAST("Total Number Of Housing Units At The Start Of This Cycle (Denominator)" AS VARCHAR)),
                ',', ''
            ) AS BIGINT
        ) AS total_housing_units_start_of_cycle,
        TRY_CAST(
            REPLACE(
                TRIM(CAST("Total Number Of New Affordable Dwelling Units In This Cycle (Numerator)" AS VARCHAR)),
                ',', ''
            ) AS BIGINT
        ) AS total_new_affordable_dwelling_units,
        -- Kept as the source's percent value (e.g. "0.005%" -> 0.005), not divided by 100
        TRY_CAST(REPLACE(TRIM(CAST("Rate Of Affordable Housing Development" AS VARCHAR)), '%', '') AS DOUBLE)
            AS rate_of_affordable_housing_development_pct,
        TRY_CAST(TRIM(CAST("AHFT Rank" AS VARCHAR)) AS INTEGER) AS ahft_rank
    FROM raw
)

SELECT * FROM final
