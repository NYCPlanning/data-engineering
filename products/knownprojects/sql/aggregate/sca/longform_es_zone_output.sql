{{ config(materialized='table', tags=['aggregate_sca']) }}

-- Project records allocated to elementary school zones.
-- es_zone_remarks falls back to the zone's remarks text when a project matched
-- no zone, trimming the trailing "Contact ..." instruction the remarks carry.

SELECT
    x.*,
    coalesce(
        x.es_zone,
        CASE
            WHEN x.es_remarks LIKE '%Contact %'
                THEN
                    substring(
                        x.es_remarks, 1, position('Contact' IN x.es_remarks) - 1
                    )
            ELSE x.es_remarks
        END
    ) AS es_zone_remarks
FROM (
    {{ longform_by_boundary(
        boundary_table='doe_eszones',
        boundary_cols=[
            {'source': 'dbn', 'alias': 'es_zone'},
            {'source': 'remarks', 'alias': 'es_remarks'}
        ],
        suffix='es_zone'
    ) }}
) AS x
ORDER BY
    x.source ASC,
    x.record_id ASC,
    x.record_name ASC,
    x.status ASC
