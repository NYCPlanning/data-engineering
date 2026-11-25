-- Mock facility condition scores (FCS) from OMB AIMS, then join them onto facdb.
-- aims_fcs_mock is shaped like the AIMS source (one row per asset, FCS_* columns)
-- so the real source can replace it without touching the join below.
--
-- TEMPORARY: replace aims_fcs_mock with the real AIMS source when it lands.
--
-- Values are derived from md5(bin) rather than random() so they're stable across
-- builds. Non-overlapping substrings of the hash keep each value independent.
-- ('x'||8hex)::bit(32)::bigint can be negative, hence abs() before mod.

-- Grades arrive pre-computed in the real source. These bands only feed the mock.
DROP FUNCTION IF EXISTS fcs_letter_grade(smallint);
CREATE FUNCTION fcs_letter_grade(score smallint) RETURNS text AS $$
BEGIN
    IF score >= 85 THEN RETURN 'A';
    ELSIF score >= 70 THEN RETURN 'B';
    ELSIF score >= 55 THEN RETURN 'C';
    ELSE RETURN 'D';
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- An AIMS asset is a building, so the mock is keyed by BIN. AIMS only surveys
-- city facilities, so candidates are city facility records with a real BIN.
-- About half of those buildings are "in AIMS" via a hash gate, numbered 1..N
-- (capped at 16000, the real AIMS asset-number range).
DROP TABLE IF EXISTS aims_fcs_mock;
CREATE TABLE aims_fcs_mock AS
WITH selected_bins AS (
    SELECT DISTINCT bin
    FROM facdb
    WHERE
        rectype = 'facility'
        AND overlevel = 'City'
        AND bin IS NOT NULL
        AND abs(('x' || substr(md5(bin::text), 1, 8))::bit(32)::bigint) % 100 < 50
),

numbered_bins AS (
    SELECT
        bin,
        row_number() OVER (ORDER BY md5(bin::text)) AS asset_id
    FROM selected_bins
),

scored AS (
    SELECT
        asset_id,
        bin,
        (abs(('x' || substr(md5(bin::text), 9, 8))::bit(32)::bigint) % 101)::smallint AS fcs_num_arc,
        (abs(('x' || substr(md5(bin::text), 17, 8))::bit(32)::bigint) % 101)::smallint AS fcs_num_sys,
        -- any day from 2010-01-01 through today
        '2010-01-01'::date + (
            abs(('x' || substr(md5(bin::text), 25, 8))::bit(32)::bigint)
            % (current_date - '2010-01-01'::date + 1)
        )::int AS fcs_asmt_dt
    FROM numbered_bins
    WHERE asset_id <= 16000
),

totaled AS (
    SELECT
        *,
        round(0.7 * fcs_num_arc + 0.3 * fcs_num_sys)::smallint AS fcs_num
    FROM scored
)

SELECT
    asset_id,
    bin,
    fcs_num,
    fcs_letter_grade(fcs_num) AS fcs_ltr,
    fcs_num_arc,
    fcs_letter_grade(fcs_num_arc) AS fcs_ltr_arc,
    fcs_num_sys,
    fcs_letter_grade(fcs_num_sys) AS fcs_ltr_sys,
    extract(YEAR FROM fcs_asmt_dt)::smallint AS fcs_asmt_yr,
    extract(MONTH FROM fcs_asmt_dt)::smallint AS fcs_asmt_mo,
    fcs_asmt_dt
FROM totaled;

ALTER TABLE facdb ADD COLUMN IF NOT EXISTS asset_id integer;
ALTER TABLE facdb ADD COLUMN IF NOT EXISTS fcs_score_total smallint;
ALTER TABLE facdb ADD COLUMN IF NOT EXISTS fcs_letter_grade_total text;
ALTER TABLE facdb ADD COLUMN IF NOT EXISTS fcs_score_architectural smallint;
ALTER TABLE facdb ADD COLUMN IF NOT EXISTS fcs_letter_grade_architectural text;
ALTER TABLE facdb ADD COLUMN IF NOT EXISTS fcs_score_systems smallint;
ALTER TABLE facdb ADD COLUMN IF NOT EXISTS fcs_letter_grade_systems text;
ALTER TABLE facdb ADD COLUMN IF NOT EXISTS fcs_assessment_year smallint;
ALTER TABLE facdb ADD COLUMN IF NOT EXISTS fcs_assessment_month smallint;
ALTER TABLE facdb ADD COLUMN IF NOT EXISTS fcs_assessment_date date;

-- Only city facility records get FCS values. A program operating inside a scored
-- building doesn't inherit them.
UPDATE facdb SET
    asset_id = m.asset_id,
    fcs_score_total = m.fcs_num,
    fcs_letter_grade_total = m.fcs_ltr,
    fcs_score_architectural = m.fcs_num_arc,
    fcs_letter_grade_architectural = m.fcs_ltr_arc,
    fcs_score_systems = m.fcs_num_sys,
    fcs_letter_grade_systems = m.fcs_ltr_sys,
    fcs_assessment_year = m.fcs_asmt_yr,
    fcs_assessment_month = m.fcs_asmt_mo,
    fcs_assessment_date = m.fcs_asmt_dt
FROM aims_fcs_mock AS m
WHERE
    facdb.bin = m.bin
    AND facdb.rectype = 'facility'
    AND facdb.overlevel = 'City';
