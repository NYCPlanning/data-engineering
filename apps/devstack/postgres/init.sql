-- One server, one DB per concern. Runs once, on an empty data volume.
-- dagster: the devstack's slim Dagster (demo location); dagster_dcpy: the opt-in
-- apps/dagster services (profile dcpy-dagster). Dispatch keeps nothing in Postgres.
CREATE DATABASE dagster;
CREATE DATABASE dagster_dcpy;
