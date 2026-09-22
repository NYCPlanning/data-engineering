# dcpy Package Structure

`dcpy` is our internal, product-agnostic Python package — utilities, connectors, and
the `lifecycle` code that orchestrates a data product from source to distribution. It's split into
separate installable packages (`dcpy-utils`, `dcpy-connectors`, `dcpy-lifecycle`, …) under
`python/*/`, joined into one [uv workspace](../development.md) and merged into a single `dcpy`
namespace package at import time — see [Installing dependencies](#installing-dependencies) below.

## Layers

dcpy is layered — **a package imports only from its own layer or a lower one:**

1. **`dcpy-utils`** (+ **`dcpy-geosupport`**) — pure, foundational packages; no dependencies on
   other dcpy packages.
2. **Everything else** — `dcpy-connectors`, `dcpy-data`, `dcpy-geospatial`,
   `dcpy-product-metadata`, `dcpy-library`: may depend on `dcpy-utils` and (where declared) each
   other, must not depend on `dcpy-lifecycle`.
3. **`dcpy-lifecycle`** — the stages (`ingest`, `builds`, `package`, `distribute`, `validate`),
   their shared base, and cross-stage `scripts`; wires everything together, and is the only
   package allowed to depend on all the others.

`dcpy.library` is deprecated (see below). This hierarchy isn't just convention: each package
declares its actual dcpy dependencies in its own `pyproject.toml`, so `uv sync --package dcpy-X`
installs only `X` and what it depends on — an import that crosses the boundary fails outright, and
CI syncs and tests every package in isolation to catch it (see [testing.md](../testing.md)).
Per-package detail, the real dependency graph, and known deviations live in the
[architecture doc](./architecture.md).

## Installing dependencies

For local dcpy development, sync the whole workspace into one shared virtual environment:

```bash
uv sync --all-packages
```

This installs every `dcpy-*` package (plus `apps/qa`, `apps/dagster`, `apps/notebook-server`)
against the committed `uv.lock`. Because everything then shares one venv, code in one package can
accidentally import a sibling that isn't a declared dependency and still pass locally — that's what
the isolated per-package CI job and each package's `test/test_package_boundary.py` exist to catch
(see [testing.md](../testing.md)). To reproduce that check locally, sync just one package instead:
`uv sync --package dcpy-<name>`.

## Data stores

`lifecycle` and `connectors` read and write three Digital Ocean (S3-compatible) stores:

- **`edm-recipes`** — our data lake / long-term store. All ingested source data is versioned
  here and never deleted; completed build outputs are archived here too. Supplies (almost) all
  source data for builds.
- **`edm-publishing`** — where build outputs land for other teams, packaging, and distribution;
  holds the full product "package" (multiple datasets, multiple formats) per version.
- **`edm-data`** — a PostgreSQL cluster used as the **build/transform engine**, not for
  persistence. A build loads source data from `edm-recipes` into it, runs transforms (mostly
  PostGIS SQL), then exports results back to `edm-publishing`. Tables persist only through a
  build cycle (useful for QA/debugging).

Full cloud inventory (apps, compute, Azure plans) is on the Cloud Infrastructure wiki page.

## Deprecated

- **`dcpy.library`** — being migrated to `dcpy.lifecycle.ingest` on a dataset-by-dataset
  basis. Do not add new templates or logic. See the
  [Library → Ingest migration guide](./library-to-ingest-migration.md).

## Testing

### Product metadata tests

Metadata now lives at the top-level `product-metadata/` directory in this repo (migrated from
the deprecated `NYCPlanning/product-metadata` standalone repo in issue #2436). Tests in
`python/product-metadata/test/` read from it by default — no extra setup is needed.

`PRODUCT_METADATA_REPO_PATH` is optional; it defaults to the in-repo `product-metadata/`
directory. Set it only if you want to point at a different checkout.

Run the tests:
```bash
uv run --no-sync pytest python/product-metadata/test/
```

These validate metadata loading/validation, the override hierarchy
(org → product → dataset → destination), template variable substitution from
`snippets/strings.yml`, destination querying/filtering, and file/column overrides.
