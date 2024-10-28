Create SCHEMA source_data;

CREATE TABLE source_data.metadata_logging (
	name text NULL,
	version text NULL,
	timestamp timestamp NULL,
	runner text NULL,
	event_source text NULL
);

CREATE INDEX source_data_metadata_logging_name_idx ON source_data.metadata_logging USING btree (name, timestamp);
CREATE INDEX source_data_metadata_logging_timestamp_idx ON source_data.metadata_logging USING btree (timestamp);
