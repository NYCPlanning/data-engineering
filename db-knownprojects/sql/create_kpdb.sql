DROP TABLE IF EXISTS _kpdb;
SELECT
    a.*,
    b.project_id,
    COALESCE(b.units_net,a.units_gross) as units_net,
    ROUND(COALESCE(a.prop_within_5_years::decimal,0) * COALESCE(b.units_net,a.units_gross)::decimal) as within_5_years,
    ROUND(COALESCE(a.prop_5_to_10_years::decimal,0) * b.units_net::decimal) as from_5_to_10_years,
    ROUND(COALESCE(a.prop_after_10_years::decimal,0) * b.units_net::decimal) as after_10_years
INTO _kpdb
FROM combined a
LEFT JOIN deduped_units b
ON a.record_id = b.record_id
WHERE a.no_classa = '0' OR a.no_classa IS NULL;

UPDATE _kpdb a
    SET borough = 
        CASE 
            WHEN a.borough IS NOT NULL THEN a.borough
            WHEN a.source = 'DCP Application' AND a.record_id like 'P%' THEN ('{"M": 1, "X": 2, "K": 3, "Q": 4, "R": 5}'::json -> substring(a.record_id, 6, 1)) :: varchar
            WHEN a.source = 'DCP Application' THEN ('{"M": 1, "X": 2, "K": 3, "Q": 4, "R": 5}'::json -> substring(a.record_id, 5, 1)) :: varchar
            WHEN a.source = 'DOB' THEN substring(a.record_id, 1, 1)
        END 
WHERE borough IS NULL;