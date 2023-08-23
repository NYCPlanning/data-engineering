/* Read in points geometry from the csv table */
DROP TABLE IF EXISTS cbbr_point_corrections;
CREATE TABLE cbbr_point_corrections (
    WKT text, --I think we can just turn this into text and then convert once we have all the geometries that were manually mapped
    OBJECTID text, --these can be deleted in preprocessing of the tables 
    unique_id text,
    editor text,
    cartodb_id text,
    shape_leng numeric


);
\COPY cbbr_point_corrections FROM 'cbbr_geom_corrections/processed/cbbr_point_corrections.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS cbbr_line_corrections;
CREATE TABLE  cbbr_line_corrections(
    WKT text,
    OBJECTID text,
    Shape_Length numeric,
    unique_id text,
    cartodb_id text,
    editor text

);
\COPY cbbr_line_corrections FROM 'cbbr_geom_corrections/processed/cbbr_line_corrections.csv' DELIMITER ',' CSV HEADER;


DROP TABLE IF EXISTS cbbr_poly_corrections;
CREATE TABLE  cbbr_poly_corrections(
    WKT text, 
    OBJECTID text,
    Shape_Length numeric,
    Shape_Area numeric,
    unique_id text,
    editor text

);
\COPY cbbr_poly_corrections FROM 'cbbr_geom_corrections/processed/cbbr_poly_corrections.csv' DELIMITER ',' CSV HEADER;



