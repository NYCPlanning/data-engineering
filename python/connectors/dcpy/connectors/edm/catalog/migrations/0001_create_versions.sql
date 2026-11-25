-- Which versions exist for an asset, per store. See dcpy.connectors.edm.catalog.
CREATE TABLE catalog.versions (
    -- what kind of asset: ingest (processed datasets) for now; builds, drafts, published later
    domain text NOT NULL,
    -- registry name of the connector the asset lives behind, e.g. edm.recipes.datasets
    store text NOT NULL,
    -- dataset or template id
    key text NOT NULL,
    -- never "latest": that's is_latest
    version text NOT NULL,
    -- e.g. s3://edm-recipes/ingest/datasets/dcp_ct2010/26c/
    location text,
    file_types text [],
    archived_at timestamptz,
    is_latest boolean NOT NULL DEFAULT false,
    -- domain-specific extras: for ingest, the dataset name, source url, ...
    details jsonb NOT NULL DEFAULT '{}',
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    refreshed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (domain, store, key, version)
);

CREATE INDEX versions_domain_key_idx ON catalog.versions (domain, key);
