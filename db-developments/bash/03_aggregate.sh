source bash/config.sh

display "Creating aggregate tables"
python3 python/yearly.py sql/aggregate/yearly.sql 2010
python3 python/yearly.py sql/aggregate/yearly.sql 2020
python3 python/yearly.py sql/aggregate/block.sql 2010
python3 python/yearly.py sql/aggregate/tract.sql 2010
python3 python/yearly.py sql/aggregate/block.sql 2020
python3 python/yearly.py sql/aggregate/tract.sql 2020
python3 python/yearly.py sql/aggregate/commntydst.sql 2010
python3 python/yearly.py sql/aggregate/councildst.sql 2010
python3 python/yearly.py sql/aggregate/nta.sql 2010
python3 python/yearly.py sql/aggregate/nta.sql 2020
python3 python/yearly.py sql/aggregate/cdta.sql 2020

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