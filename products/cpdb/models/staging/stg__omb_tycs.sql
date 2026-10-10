-- Four releases don't match the published column layout, so each is realigned by pub_date.
-- 20230112 and 20250116 dropped the agency name; it's filled from the same cycle's
-- final release, which agrees with every other release on it.
WITH realigned AS (
    SELECT
        pub_date,
        CASE
            WHEN pub_date IN ('20230426', '20250501') THEN projtype_desc
            ELSE projtype
        END AS projtype,
        CASE
            WHEN pub_date IN ('20230112', '20250116') THEN NULL
            WHEN pub_date IN ('20230426', '20250501') THEN projtype
            ELSE projtype_desc
        END AS projtype_desc,
        CASE
            WHEN pub_date IN ('20230112', '20250116') THEN projtype_desc
            ELSE typc
        END AS typc,
        CASE WHEN pub_date = '20230112' THEN typc ELSE funding END AS funding,
        CASE WHEN pub_date = '20230112' THEN funding ELSE yr1_fy END AS yr1_fy,
        CASE WHEN pub_date = '20230112' THEN yr1_fy ELSE year1_amt END AS year1_amt,
        CASE WHEN pub_date = '20230112' THEN year1_amt ELSE year2_amt END AS year2_amt,
        CASE WHEN pub_date = '20230112' THEN year2_amt ELSE year3_amt END AS year3_amt,
        CASE WHEN pub_date = '20230112' THEN year3_amt ELSE year4_amt END AS year4_amt,
        CASE WHEN pub_date = '20230112' THEN year4_amt ELSE year5_amt END AS year5_amt,
        CASE WHEN pub_date = '20230112' THEN year5_amt ELSE year6_amt END AS year6_amt,
        CASE WHEN pub_date = '20230112' THEN year6_amt ELSE year7_amt END AS year7_amt,
        CASE WHEN pub_date = '20230112' THEN year7_amt ELSE year8_amt END AS year8_amt,
        CASE WHEN pub_date = '20230112' THEN year8_amt ELSE year9_amt END AS year9_amt,
        CASE WHEN pub_date = '20230112' THEN year9_amt ELSE year10_amt END AS year10_amt,
        CASE WHEN pub_date = '20230112' THEN year10_amt ELSE total END AS total
    FROM {{ source('recipe_sources', 'omb_tycs') }}
),

final_release AS (
    SELECT * FROM (VALUES ('20230112', '20230426'), ('20250116', '20250501'))
        AS t (pub_date, final_pub_date)
),

-- PU has two agency names within a release, so match on category before project type
agency_by_category AS (
    SELECT DISTINCT
        pub_date,
        projtype,
        typc,
        projtype_desc
    FROM realigned
    WHERE projtype_desc IS NOT NULL
),

agency_by_projtype AS (
    SELECT
        pub_date,
        projtype,
        min(projtype_desc) AS projtype_desc
    FROM realigned
    WHERE projtype_desc IS NOT NULL
    GROUP BY pub_date, projtype
    HAVING count(DISTINCT projtype_desc) = 1
),

filled AS (
    SELECT
        r.*,
        coalesce(r.projtype_desc, c.projtype_desc, p.projtype_desc) AS filled_projtype_desc
    FROM realigned AS r
    LEFT JOIN final_release AS e ON r.pub_date = e.pub_date
    LEFT JOIN agency_by_category AS c
        ON
            e.final_pub_date = c.pub_date
            AND r.projtype = c.projtype
            AND r.typc = c.typc
    LEFT JOIN agency_by_projtype AS p
        ON
            e.final_pub_date = p.pub_date
            AND r.projtype = p.projtype
)

-- In 2021 two codes shared a category name (RE MSC and NWP, merged into MSC by 2023), so the
-- strategy has two rows for that name. Rows are summed to one per category name.
SELECT
    to_date(pub_date, 'YYYYMMDD') AS pub_date,
    projtype,
    filled_projtype_desc AS projtype_desc,
    typc,
    funding,
    yr1_fy::integer AS yr1_fy,
    sum(year1_amt::numeric) AS year1_amt,
    sum(year2_amt::numeric) AS year2_amt,
    sum(year3_amt::numeric) AS year3_amt,
    sum(year4_amt::numeric) AS year4_amt,
    sum(year5_amt::numeric) AS year5_amt,
    sum(year6_amt::numeric) AS year6_amt,
    sum(year7_amt::numeric) AS year7_amt,
    sum(year8_amt::numeric) AS year8_amt,
    sum(year9_amt::numeric) AS year9_amt,
    sum(year10_amt::numeric) AS year10_amt,
    sum(total::numeric) AS total
FROM filled
GROUP BY pub_date, projtype, filled_projtype_desc, typc, funding, yr1_fy
