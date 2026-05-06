{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs', 'diffs_thinlion']
  )
}}

-- Compressed diff view showing one row per atomicid with changes in jsonb
-- Includes status: modified, only_in_legacy, only_in_build

{{ generate_diff_summary(
    old_relation=ref('qa_int__prod_thinlion_manhattan'),
    new_relation=ref('thinlion_manhattan_by_field'),
    primary_key='atomicid',
    output_file_id='thinlion_manhattan',
    build_table_name='thinlion_manhattan_by_field',
    production_table_name='qa_int__prod_thinlion_manhattan'
) }}
