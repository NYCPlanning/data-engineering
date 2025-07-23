CREATE EXTENSION IF NOT EXISTS tablefunc;

DROP TABLE IF EXISTS type_category_patterns CASCADE;
CREATE TABLE type_category_patterns (
    pattern character varying,
    typecategory character varying
);

CREATE TEMP TABLE staging_patterns (
    pattern character varying
);

\COPY staging_patterns FROM 'data/type_patterns_ittvequ.csv' DELIMITER ',' CSV;
INSERT INTO type_category_patterns (pattern, typecategory)
SELECT
    pattern,
    'ITT, Vehicles, and Equipment' AS typecategory
FROM staging_patterns;
DELETE FROM staging_patterns;

\COPY staging_patterns FROM 'data/type_patterns_lump_sum.csv' DELIMITER ',' CSV;
INSERT INTO type_category_patterns (pattern, typecategory)
SELECT
    pattern,
    'Lump Sum' AS typecategory
FROM staging_patterns;
DELETE FROM staging_patterns;

\COPY staging_patterns FROM 'data/type_patterns_fixed_asset.csv' DELIMITER ',' CSV;
INSERT INTO type_category_patterns (pattern, typecategory)
SELECT
    pattern,
    'Fixed Asset' AS typecategory
FROM staging_patterns;
