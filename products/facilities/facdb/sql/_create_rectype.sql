-- Assign RECTYPE (facility / program) from the factype_rectype_mapping seed.
-- The seed is the source of truth for valid values; a blank rectype or an
-- unmapped factype yields NULL.

ALTER TABLE facdb ADD COLUMN IF NOT EXISTS rectype text;

-- Reset first so a standalone re-run is deterministic (unmapped factypes -> NULL).
UPDATE facdb SET rectype = NULL;

UPDATE facdb
SET rectype = nullif(lower(m.rectype), '')
FROM factype_rectype_mapping AS m
WHERE facdb.factype = m.factype;
