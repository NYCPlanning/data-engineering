CREATE OR REPLACE TABLE nypdprecinct AS SELECT * FROM read_parquet('{data}/nypdprecinct.parquet');
CREATE OR REPLACE TABLE nypdpatrolborough AS SELECT * FROM read_parquet('{data}/nypdpatrolborough.parquet');
CREATE OR REPLACE TABLE nypdbeat AS SELECT * FROM read_parquet('{data}/nypdbeat.parquet');
