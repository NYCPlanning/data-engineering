-- especially concerned about diffs in the order of rows within each generic_segmentid grouping
{% set old_query %}
  select
    ROW_NUMBER() OVER () as row_number,
    generic_segmentid || '_' || ROW_NUMBER() OVER (
            PARTITION BY generic_segmentid
        ) AS rpl_id,
    *
  from production_outputs.rpl
  order by row_number asc
{% endset %}

{% set new_query %}
  select
      ROW_NUMBER() OVER () as row_number,
      lpad(rpl_id, 9, '0') as rpl_id,
      generic_segmentid,
      roadbed_segmentid,
      roadbed_position_code
  from {{ ref('rpl_by_field') }}
  order by row_number asc
{% endset %}


{{ 
  audit_helper.compare_and_classify_query_results(
    old_query,
    new_query,
    primary_key_columns=['rpl_id'], 
    columns=['generic_segmentid', 'roadbed_segmentid', 'roadbed_position_code'],
    sample_limit=50
  )
}}
