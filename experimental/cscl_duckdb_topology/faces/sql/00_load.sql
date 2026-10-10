-- Inputs from Parquet into in-memory tables (stand-ins for the dbt refs).
CREATE OR REPLACE TABLE edges AS SELECT * FROM read_parquet('{data}/edges.parquet');
CREATE OR REPLACE TABLE ap_entities AS SELECT * FROM read_parquet('{data}/ap_entities.parquet');
CREATE OR REPLACE TABLE atomicpolygons AS SELECT * FROM read_parquet('{data}/atomicpolygons.parquet');
