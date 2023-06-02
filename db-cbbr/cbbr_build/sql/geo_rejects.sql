DROP TABLE IF EXISTS geo_rejects;

CREATE TABLE geo_rejects AS (
    SELECT
        unique_id,
        address,
        street_name,
        between_cross_street_1,
        and_cross_street_2,
        borough,
        geo_message
    FROM
        _cbbr_submissions
    WHERE (address != ' ' OR street_name != ' ')
    AND geom IS NULL
);

