-- -- no longer need to do because ap_date now comes in MM/DD/YY format
-- -- converting datetime from year and day of year e.g. 93005 to 1993-01-05 then MM/DD/YYYY
-- UPDATE pluto_rpad_geo
-- SET ap_datef = to_char(to_date(lpad(ap_date, 5, '0'), 'YYDDD'), 'MM/DD/YYYY')
-- WHERE ap_date::numeric > 0;

-- format from MM/DD/YY to MM/DD/YYYY
UPDATE pluto_rpad_geo
SET ap_datef = to_char(to_date(ap_date, 'MM/DD/YY'), 'MM/DD/YYYY')
WHERE ap_date IS NOT NULL;