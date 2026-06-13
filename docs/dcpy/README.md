# dcpy Package Structure

`dcpy` is our internal, product-agnostic Python package — utilities, connectors, and
the `lifecycle` code that orchestrates a data product from source to distribution.
(Longer term we hope to make it a publicly installable package.)

Follow the layer rules strictly.

## Layer Architecture

### Layer 1: utils
**Most foundational. Import-safe for all modules.**

- Relatively pure utilities — simple, atomic functions with no business logic
- No dependencies on other dcpy modules
- Alter with extreme caution; ALWAYS unit test changes (aim for >90% coverage)

### Layer 2: Everything else (except lifecycle)
**Can import: utils. Should not import each other.** Cannot import from `lifecycle`.

- `connectors/` — interfaces to entities outside dcpy: our own resources (`edm.recipes`,
  `edm.publishing`) and third parties (Socrata, the API behind [NYC OpenData](https://opendata.cityofnewyork.us/);
  ArcGIS Online). Operations should be atomic (get a dataset, push a dataset) and free of
  business logic.
- `geosupport/` — geospatial helpers and Geosupport bindings
- [`product_metadata/`](../../dcpy/product_metadata/README.md) — product configuration and dataset documentation
- `models/` — Pydantic classes representing discrete entities (a dataset, a recipe, etc.),
  often parsed from YAML/JSON. Organized by domain; partly mirrors the rest of dcpy. No code
  beyond class/object methods, and no dependencies on other submodules — so every submodule
  can reference these entities without circular imports. **(deprecated for new code — see below)**
- `library/` — the original CLI for extracting and archiving source data from YAML templates.
  Being replaced by `lifecycle.ingest`. **(deprecated — see below)**

### Layer 3: lifecycle
**Can import: everything below. Stages should avoid importing other stages.**

The heart of dcpy: generalized tooling for the stages of a product's lifecycle — no logic
specific to any one dataset, just tooling for how these entities relate and feed into one
another. Stages:

- `ingest/` — extract, normalize, and archive source data to `edm-recipes`. Driven by YAML
  templates; `run.py` holds the top-level template→archive flow (look up a version, download
  raw data, archive it, convert to parquet, run preprocessing/reprojection, archive the
  processed dataset). Replaces `library`.
- `builds/` — *prepares* a build: "plans" it from a product's [`recipe.yml`](../../products/green_fast_track/recipe.yml)
  and loads source datasets into the build Postgres database. (The transforms themselves still
  run in the product folders via bash/sql/dbt, then lifecycle code picks back up.)
- `package/` — bundles a build's outputs and metadata for distribution (annotations,
  attachments, etc.)
- `distribute/` — pushes exports to external destinations (mainly Socrata/OpenData)
- `validate/` — quality checks
- `scripts/` — cross-stage utilities (prefer this for code shared across stages)

**Purpose:** wire other parts of the codebase together (e.g. configuring a generic connector
for a specific job, instantiating a locally cloned metadata repo). Avoid heavy business logic here.

## Import Rules

```python
# ✅ Allowed
from dcpy.utils import ...
from dcpy.library import ...  # in layer 2+ only
from dcpy.lifecycle.scripts import ...  # in lifecycle only

# ❌ Not allowed
from dcpy.lifecycle import ...  # in layer 1 or 2
from dcpy.models import ...  # anywhere (deprecated)
```

## Deprecated

- **`dcpy.models`** — do not add new code. Move models out when possible.
- **`dcpy.library`** — being migrated to `dcpy.lifecycle.ingest` on a dataset-by-dataset
  basis. Do not add new templates or logic. See the
  [Library → Ingest migration guide](https://github.com/NYCPlanning/data-engineering/wiki/Library-to-Ingest-Migration).

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
