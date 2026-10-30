"""dagster-broker: in-memory Valkey lease queue that routes Dagster work to HTTP-only workers.

Modules: app + store (the server), client (stdlib HTTP client), worker (the worker loop),
pipes_worker (dagster-pipes message writer for step subprocesses), pipes_dagster (the
Dagster-side Pipes client). Importing this package imports none of them."""
