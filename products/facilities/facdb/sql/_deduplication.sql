-- Create empty table -> facdb_duplicates, same schema as facdbs
DROP TABLE IF EXISTS facdb_duplicates;
SELECT * INTO facdb_duplicates FROM facdb LIMIT 0; --noqa: AM09

-- Within source deduplication -> same bin or geom, facname, factype, and datasource
-- TODO - these row_number without ORDER BY is not deterministic
--        this should be fixed, but with some attention to how we're ordering to ensure it's deterministic
DELETE FROM facdb
WHERE uid IN (
    SELECT uid FROM (
        SELECT
            uid,
            ROW_NUMBER() OVER (
                PARTITION BY
                    COALESCE(bin::Text, geom::Text),
                    factype,
                    datasource,
                    REGEXP_REPLACE(facname, '[^a-zA-Z0-9]+', '', 'g'),
                    opabbrev
            ) AS rownum
        FROM facdb
    ) AS a
    WHERE rownum > 1
);

-- For factype NYCHA COMMUNITY CENTER - CHILD CARE,
-- if dohmh_daycare has a site with the same BIN, delete the nycha record
DELETE FROM facdb
WHERE
    factype = 'NYCHA COMMUNITY CENTER - CHILD CARE'
    AND datasource = 'nycha_communitycenters'
    AND bin IS NOT NULL
    AND bin IN (
        SELECT DISTINCT bin FROM facdb
        WHERE datasource = 'dohmh_daycare'
    );

-- For factype NYCHA COMMUNITY CENTER - SENIOR CENTER,
-- If dfta_contracts has a site with the same BIN, delete the nycha record
DELETE FROM facdb
WHERE
    factype = 'NYCHA COMMUNITY CENTER - SENIOR CENTER'
    AND datasource = 'nycha_communitycenters'
    AND bin IS NOT NULL
    AND bin IN (
        SELECT DISTINCT bin FROM facdb
        WHERE datasource = 'dfta_contracts'
    );

/* For factype DAY CARE,
	if record from any source has same BIN, facname has no numbers,
	and facname matches within 3 non-special characters, delete lowest
	uid.

	here we are saving the duplicates in facdb_duplicates for review
*/
INSERT INTO facdb_duplicates
SELECT * FROM facdb
WHERE
    facsubgrp = 'DAY CARE'
    AND bin IS NOT NULL
    AND uid IN (
        SELECT a.uid
        FROM facdb AS a
        INNER JOIN facdb AS b
            ON
                a.uid > b.uid
                AND a.bin = b.bin
                AND a.facsubgrp = b.facsubgrp
                AND a.facsubgrp = 'DAY CARE'
                AND LEVENSHTEIN(
                    UPPER(REGEXP_REPLACE(a.facname, '[^a-zA-Z0-9]+', '', 'g')),
                    UPPER(REGEXP_REPLACE(b.facname, '[^a-zA-Z0-9]+', '', 'g'))
                )
                <= 3
                AND UPPER(REGEXP_REPLACE(a.facname, '[^a-zA-Z0-9]+', '', 'g'))
                <> UPPER(REGEXP_REPLACE(b.facname, '[^a-zA-Z0-9]+', '', 'g'))
                AND a.facname ~ '^[^0-9]+$'
                AND b.facname ~ '^[^0-9]+$'
    );

/* For facsubgrp Youth Centers, Literacy Programs, and Job Training Services
	If two records have the same bin, and fuzzy matching for facnames without
	numbers and the difference is between 2 and 3 characters
	(resolves MS and PS false deduplication) OR the facnames are exactly
	 the same after removing punctuation, capitalization, and spaces
	here we are saving the duplicates in facdb_duplicates for review
*/
INSERT INTO facdb_duplicates
SELECT * FROM facdb
WHERE
    bin IS NOT NULL
    AND uid IN (
        SELECT a.uid
        -- a.bin,
        -- a.factype as factype_a,
        -- b.factype as factype_b,
        -- a.datasource as datasource_a,
        -- b.datasource as datasource_b,
        -- a.facname as facname_a,
        -- b.facname as facname_b,
        -- levenshtein(UPPER(regexp_replace(a.facname, '[^a-zA-Z0-9]+', '','g')),UPPER(regexp_replace(b.facname, '[^a-zA-Z0-9]+', '','g')))
        FROM facdb AS a INNER JOIN facdb AS b ON a.uid > b.uid
        -- Same BIN and not million BIN
        AND a.bin::Text NOT LIKE '%000000'
        AND a.bin = b.bin
        -- First part of factypes match
        AND LEFT(a.factype, STRPOS(a.factype, ':') - 1) = LEFT(b.factype, STRPOS(b.factype, ':') - 1)
        -- Subgroup is 'Youth Centers, Literacy Programs, and Job Training Services'
        AND a.facsubgrp ~* 'Youth Centers, Literacy Programs, and Job Training Services'
        -- There is fuzzy matching for facnames without numbers and the difference must be between 2 and 3 characters (resolves MS and PS false deduplication)
        AND (
            (
                LEVENSHTEIN(
                    UPPER(REGEXP_REPLACE(a.facname, '[^a-zA-Z0-9]+', '', 'g')),
                    UPPER(REGEXP_REPLACE(b.facname, '[^a-zA-Z0-9]+', '', 'g'))
                ) BETWEEN 2 AND 3
                AND a.facname ~ '^[^0-9]+$' AND b.facname ~ '^[^0-9]+$'
            )
            -- OR the facnames are exactly the same after removing punctuation, capitalization, and spaces
            OR UPPER(REGEXP_REPLACE(a.facname, '[^a-zA-Z0-9]+', '', 'g'))
            = UPPER(REGEXP_REPLACE(b.facname, '[^a-zA-Z0-9]+', '', 'g'))
        )
    );

/* For Charter Schools,
	if a nysed_activeinstitutions record has a match with a doe_lcgms
	where the names are the same and facsubgrp='Charter K-12 Schools'
	and the locations are the same, delete nysed_activeinstitutions record.

	here we are saving the duplicates in facdb_duplicates for review
*/
INSERT INTO facdb_duplicates
WITH doe AS (
    SELECT * FROM facdb
    WHERE datasource = 'doe_lcgms' AND facsubgrp = 'CHARTER K-12 SCHOOLS'
),
nysed AS (
    SELECT * FROM facdb
    WHERE datasource = 'nysed_activeinstitutions' AND facsubgrp = 'CHARTER K-12 SCHOOLS'
)
SELECT nysed.*
FROM nysed
INNER JOIN doe ON
    nysed.bin = doe.bin
    AND CLEAN_CHARTER_SCHOOL_NAME(nysed.facname) = CLEAN_CHARTER_SCHOOL_NAME(doe.facname);

-- Remove records outside of NYC based on geometry
DELETE FROM facdb
WHERE geom IS NOT NULL AND uid NOT IN (
    SELECT a.uid FROM facdb AS a, (
        SELECT ST_UNION(wkb_geometry) AS geom FROM dcp_boroboundaries_wi
    ) AS b
    WHERE ST_CONTAINS(ST_SETSRID(b.geom, 4326), a.geom)
);

-- remove all facdb_duplicates records in facdb
DELETE FROM facdb
WHERE uid IN (SELECT DISTINCT uid FROM facdb_duplicates);
