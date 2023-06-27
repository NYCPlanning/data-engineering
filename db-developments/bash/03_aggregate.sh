source bash/config.sh

display "Clean census data"
run_sql_file sql/_census.sql

display "Creating yearly aggregate tables"
python3 python/aggregate.py sql/aggregate/yearly.sql 2020

display "Creating spatial aggregate tables"
python3 python/aggregate.py sql/aggregate/spatial.sql 2020 block
python3 python/aggregate.py sql/aggregate/spatial.sql 2020 tract
python3 python/aggregate.py sql/aggregate/spatial.sql 2020 commntydst
python3 python/aggregate.py sql/aggregate/spatial.sql 2020 councildst
python3 python/aggregate.py sql/aggregate/spatial.sql 2020 nta
python3 python/aggregate.py sql/aggregate/spatial.sql 2020 cdta
