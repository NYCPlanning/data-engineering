/*
This SQL file contains queries for creating the event_logging table.
The table is used to track the lifecycle of data products, 
from building to publishing, and is part of the de-qaqc database.
*/

CREATE SCHEMA product_data;

CREATE TABLE product_data.event_logging (
    product VARCHAR(50) NOT NULL,
    version VARCHAR(20) NOT NULL,
    event VARCHAR(30) NOT NULL,
    path TEXT NOT NULL,
    old_path TEXT,
    runner_type VARCHAR,
    runner VARCHAR,
    timestamp TIMESTAMP NOT NULL,
    custom_fields JSONB
);

CREATE INDEX idx_product_version_path ON product_data.event_logging (product, version, path);
