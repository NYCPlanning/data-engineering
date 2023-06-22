source bash/config.sh

display "Creating aggregate tables"
python3 python/aggregate.py sql/aggregate/yearly.sql 2010
python3 python/aggregate.py sql/aggregate/yearly.sql 2020
python3 python/aggregate.py sql/aggregate/spatial.sql 2010 block
python3 python/aggregate.py sql/aggregate/spatial.sql 2010 tract
python3 python/aggregate.py sql/aggregate/spatial.sql 2020 block
python3 python/aggregate.py sql/aggregate/spatial.sql 2020 tract
python3 python/aggregate.py sql/aggregate/spatial.sql 2010 commntydst
python3 python/aggregate.py sql/aggregate/spatial.sql 2010 councildst
python3 python/aggregate.py sql/aggregate/spatial.sql 2010 nta
python3 python/aggregate.py sql/aggregate/spatial.sql 2020 nta
python3 python/aggregate.py sql/aggregate/spatial.sql 2020 cdta

mkdir -p output && (
    display "Export aggregate tables"
    python3 python/clean_export_aggregate.py aggregate_block_2020 &
    python3 python/clean_export_aggregate.py aggregate_tract_2020 &
    python3 python/clean_export_aggregate.py aggregate_nta_2020 &
    python3 python/clean_export_aggregate.py aggregate_councildst_2010 &
    python3 python/clean_export_aggregate.py aggregate_commntydst_2010  &
    python3 python/clean_export_aggregate.py aggregate_cdta_2020 & 
    wait
)