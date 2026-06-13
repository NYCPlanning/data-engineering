# dcpy Package Structure

`dcpy` is our internal, product-agnostic Python package — utilities, connectors, and
the `lifecycle` code that orchestrates a data product from source to distribution.

For the desired import flow, the full layer ordering, enforcement
via `tach`, and known deviations, see [Architecture & Import Flow](./architecture.md).

## Layers

dcpy is layered — **a module imports only from its own layer or a lower one:**

1. **`utils`** — pure, foundational utilities; no dependencies on other dcpy modules.
2. **Everything else** — `connectors`, `geosupport`, `models`, `product_metadata`, `data`,
   `library`: may import `utils`, must not import `lifecycle`, and ideally not each other.
3. **`lifecycle`** — the stages (`ingest`, `builds`, `package`, `distribute`, `validate`), their
   shared base, and cross-stage `scripts`; wires everything together.

`models` and `library` are deprecated (see below). Per-submodule and per-stage detail, the import
rules, `tach` enforcement, and known deviations live in the [architecture doc](./architecture.md).

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

- **`dcpy.models`** — do not add new code. Move models out when possible.
- **`dcpy.library`** — being migrated to `dcpy.lifecycle.ingest` on a dataset-by-dataset
  basis. Do not add new templates or logic. See the
  [Library → Ingest migration guide](./library-to-ingest-migration.md).

## Testing

### Product metadata tests

Tests in `dcpy/test/product_metadata/` require a local clone of the
[product-metadata](https://github.com/NYCPlanning/product-metadata) repository:

1. Clone the repo:
   ```bash
   git clone https://github.com/NYCPlanning/product-metadata.git ~/product-metadata
   ```
2. Point `PRODUCT_METADATA_REPO_PATH` at it (add to your shell profile to persist):
   ```bash
   export PRODUCT_METADATA_REPO_PATH=~/product-metadata
   ```
3. Run the tests:
   ```bash
   pytest dcpy/test/product_metadata/
   ```

These validate metadata loading/validation, the override hierarchy
(org → product → dataset → destination), template variable substitution from
`snippets/strings.yml`, destination querying/filtering, and file/column overrides.
