-- especially concerned about diffs in the order of rows within each generic_segmentid grouping.
-- Dev's order is RPL.txt's: rpl_fields (the export's layout_source) in the export's
-- order_by (var rpl_file_order_by, equal to recipe.yml's), not the model's own order.
{% set old_query %}
  select
    ROW_NUMBER() OVER () as row_number,
    generic_segmentid || '_' || ROW_NUMBER() OVER (
            PARTITION BY generic_segmentid
        ) AS rpl_id,
    *
  from {{ source('production_outputs', 'rpl') }}
  order by row_number asc
{% endset %}

{% set new_query %}
  with file_order as (
    select
      ROW_NUMBER() OVER (ORDER BY {{ var('rpl_file_order_by') }}) as row_number,
      lpad(generic_segmentid::text, 7, '0') as generic_segmentid,
      lpad(roadbed_segmentid::text, 7, '0') as roadbed_segmentid,
      roadbed_position_code
    from {{ ref('rpl_fields') }}
  )
  select
      row_number,
      generic_segmentid || '_' || ROW_NUMBER() OVER (
          PARTITION BY generic_segmentid ORDER BY row_number
      ) as rpl_id,
      generic_segmentid,
      roadbed_segmentid,
      roadbed_position_code
  from file_order
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
