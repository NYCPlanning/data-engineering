-- Add mock SGR (State of Good Repair) columns to the facdb table.
-- The asset id and SGR values are derived deterministically from md5(bin::text)
-- so that (a) every record for the same building (BIN) gets identical values and
-- (b) values are stable across nightly builds. No random() calls; no setseed.
--
-- TEMPORARY: remove this file when the real AIMS SGR source lands (scores/grades
-- arrive pre-computed).

-- Helper function: letter grade from 0–100 score.
-- TEMPORARY/mock — not canonical grade logic. Bands live upstream (omb-aims)
-- where the real SGR source is produced; this is only for generating mock data.
DROP FUNCTION IF EXISTS sgr_grade(smallint);
CREATE FUNCTION sgr_grade(score smallint) RETURNS text AS $$
BEGIN
    IF score >= 90 THEN RETURN 'A';
    ELSIF score >= 75 THEN RETURN 'B';
    ELSIF score >= 60 THEN RETURN 'C';
    ELSIF score >= 50 THEN RETURN 'D';
    ELSE RETURN 'F';
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

-- Add columns (IF NOT EXISTS for idempotency on re-runs).
ALTER TABLE facdb ADD COLUMN IF NOT EXISTS asset_id integer;
ALTER TABLE facdb ADD COLUMN IF NOT EXISTS sgr_score_arch smallint;
ALTER TABLE facdb ADD COLUMN IF NOT EXISTS sgr_grade_arch text;
ALTER TABLE facdb ADD COLUMN IF NOT EXISTS sgr_score_syst smallint;
ALTER TABLE facdb ADD COLUMN IF NOT EXISTS sgr_grade_syst text;
ALTER TABLE facdb ADD COLUMN IF NOT EXISTS sgr_score_tot smallint;
ALTER TABLE facdb ADD COLUMN IF NOT EXISTS sgr_grade_tot text;
ALTER TABLE facdb ADD COLUMN IF NOT EXISTS sgr_assmnt_year smallint;

-- ASSET_ID: mock AIMS asset number. An AIMS asset is a building, so the id is
-- keyed off BIN (not uid): every record sharing a BIN gets the same asset_id and
-- the same SGR values below. Eligible buildings are CITY facility records with a
-- real BIN: AIMS only surveys city facilities, so State/Federal facilities are
-- excluded here even though their rectype is 'facility'. The placeholder
-- "million BINs" are already NULL in facdb (see _create_facdb_spatial.sql).
-- Selection: ~50% of eligible buildings are "in AIMS", via a deterministic hash
-- gate on BIN. Each selected building gets a UNIQUE id from a hash-ordered ranking
-- (1..N), capped at 16000 to match the real AIMS asset-number range. asset_id is
-- 1:1 with BIN, mirroring the AIMS source — no two BINs share an id.
-- Only city facility records receive an asset_id/SGR (the final UPDATE filters
-- rectype = 'facility' and overlevel = 'City'). A program operating inside a
-- scored building does NOT inherit its values — SGR shows only on the facility.
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
)

UPDATE facdb
SET asset_id = numbered_bins.asset_id
FROM numbered_bins
WHERE
    facdb.bin = numbered_bins.bin
    AND facdb.rectype = 'facility'
    AND facdb.overlevel = 'City'
    AND numbered_bins.asset_id <= 16000;

-- Scores and assessment year are also keyed off BIN, so all records for the same
-- building share identical SGR. Non-overlapping md5(bin) substrings keep the three
-- values independent. ('x'||8hex)::bit(32)::bigint can be negative; abs() then mod.
UPDATE facdb SET
    sgr_score_arch = (abs(('x' || substr(md5(bin::text), 9, 8))::bit(32)::bigint) % 101)::smallint,
    sgr_score_syst = (abs(('x' || substr(md5(bin::text), 17, 8))::bit(32)::bigint) % 101)::smallint,
    -- Year: chars 25–32; map to [2010, current_year] inclusive.
    sgr_assmnt_year = (
        2010 + abs(('x' || substr(md5(bin::text), 25, 8))::bit(32)::bigint)
        % (extract(YEAR FROM current_date)::int - 2010 + 1)
    )::smallint
WHERE asset_id IS NOT NULL;

-- Total score: weighted average of arch and syst
UPDATE facdb SET
    sgr_score_tot = round(0.6 * sgr_score_arch + 0.4 * sgr_score_syst)::smallint
WHERE asset_id IS NOT NULL;

-- Letter grades derived from scores
UPDATE facdb SET
    sgr_grade_arch = sgr_grade(sgr_score_arch),
    sgr_grade_syst = sgr_grade(sgr_score_syst),
    sgr_grade_tot = sgr_grade(sgr_score_tot)
WHERE asset_id IS NOT NULL;
