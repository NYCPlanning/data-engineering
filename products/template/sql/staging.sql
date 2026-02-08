-- create staging versions of source data

CREATE TABLE stg_nypl_libraries AS SELECT
    name AS library_name,
    locality AS borough,
    wkb_geometry
FROM nypl_libraries;

CREATE TABLE stg_bpl_libraries AS SELECT
    title AS library_name,
    wkb_geometry
FROM bpl_libraries;

CREATE TABLE stg_qpl_libraries AS SELECT
    name AS library_name,
    geom AS wkb_geometry
FROM qpl_libraries;

CREATE TABLE stg_dpr_parksproperties AS SELECT
    signname AS space_name,
    borough,
    wkb_geometry
FROM dpr_parksproperties;

CREATE TABLE stg_dpr_greenthumb AS SELECT
    gardenname AS space_name,
    borough,
    wkb_geometry
FROM dpr_greenthumb;

CREATE TABLE stg_lpc_landmarks AS SELECT
    lm_name AS landmark_name,
    boroughid AS borough_name_short,
    bbl,
    wkb_geometry
FROM lpc_landmarks;

CREATE TABLE boroughs (
    name text,
    name_short text,
    bbl_code text
);

\COPY boroughs FROM 'seeds/boroughs.csv' DELIMITER ',' CSV;
