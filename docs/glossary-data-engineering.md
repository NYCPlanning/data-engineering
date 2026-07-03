# Data Engineering Glossary

Definitions for terms and concepts used across Data Engineering products and pipelines.

## Data Engineering

### Core concepts

**Product**: A product is a body of work the team builds and maintains under `products/<name>/`, carrying data through the Ingest → Build → Draft → QA → Publish pipeline to public distribution. See **Recipe** for how a product declares its inputs and outputs.

**Dataset**: A dataset is a single named, versioned unit of data — either a source dataset consumed as input to a build, or a product's exported output in a given format. It's the unit that **Ingest**, `edm-recipes`, and a product's `recipe.yml` all operate on.

**Version**: A version is the identifier attached to a dataset or a data product **Release** (e.g. `20240105`, `24Q3`). Ingest archives a new version of a source dataset each time it changes; a product's own `version` in its `recipe.yml` identifies a specific release of that product.

**Release**: A release is a product's output once it has been published and distributed to its configured **Destination**(s) — the outcome of the **Publish** stage.

**Freshness check**: A freshness check is a manual judgment call on whether a source dataset is recent enough to use in a **Release**.

**Destination**: A destination is a named target a product's output is distributed to (e.g. a specific Socrata dataset, an Open Data endpoint) — declared in `recipe.yml` under `distribution.destination_ids` and configured in that product's metadata.

**Recipe**: A recipe is a product's `recipe.yml` — it declares the product's input datasets and versions, the build stage's commands and destination, its exported file formats, and its distribution destinations, and is read by `dcpy.lifecycle.builds` to plan and run a build. Not to be confused with `edm-recipes`, the S3 store: a recipe is a per-product config file, while `edm-recipes` is where the datasets it references are archived.

**Product metadata**: Product metadata is a product's dataset specifications (`metadata.yml`), living under `product-metadata/products/<name>/` in this repo — it describes destinations, columns, and overrides for distribution.

### Pipeline stages

**Ingest**: Ingest is the pipeline stage that extracts a source dataset (from an API or file) and archives it to `edm-recipes`, versioned. Implemented in `dcpy.lifecycle.ingest`.

**Ingest template**: An ingest template is a YAML spec under `ingest_templates/` describing how to fetch and archive a given source dataset — what the Ingest stage runs against.

**Build**: A build is the pipeline stage that loads a product's archived source datasets from `edm-recipes` into Postgres (`edm-data`) and runs its dbt/SQL transforms. Implemented in `dcpy.lifecycle.builds`.

**Draft**: A draft is the pipeline stage that promotes a build's output to the S3 `draft` folder in `edm-publishing` and runs automated **QA checks**. Roughly corresponds to `dcpy.lifecycle.package`, which bundles a build's outputs and metadata for distribution.

**QA**: QA is the pipeline stage where domain experts and the GIS team review a product's draft output; issues found here send it back for rebuild. Not to be confused with **QA checks** (automated, run earlier) or the **`qaqc` app** (a tool used during this stage).

**QA checks**: QA checks are the automated checks run during the **Draft** stage, before a product's output reaches human **QA** review. Implemented in `dcpy.lifecycle.validate`.

**`qaqc` app**: The `qaqc` app is a Streamlit app (`apps/`, served at `/qaqc`) providing product comparison pages for PLUTO, DevDB, FacDB, and others — used to support the **QA** review stage.

**Publish**: Publish is the pipeline stage that promotes an approved draft to a product's configured **Destination**(s), producing a **Release**. Implemented in `dcpy.lifecycle.distribute`.

### Infrastructure

**`dcpy`**: `dcpy` is the team's internal, product-agnostic Python package — utilities, connectors, and the `lifecycle` code that orchestrates a product from source to distribution.

**`edm-recipes`**: `edm-recipes` is the team's data lake / long-term store (Digital Ocean S3-compatible). All ingested source datasets are versioned here and never deleted; it supplies almost all source data for builds.

**`edm-data`**: `edm-data` is a Postgres cluster used as the build/transform engine (not for persistence) — a build loads source data into it, runs transforms, then exports results back to `edm-publishing`.

**`edm-publishing`**: `edm-publishing` is where build outputs land — it holds the full product "package" (multiple datasets, multiple formats) per version, organized into `draft` and `publish` folders.

## See also

- [Geosupport UPG Glossary](https://nycplanning.github.io/Geosupport-UPG/appendices/glossary/) — terminology from the Geosupport User Programming Guide (UPG)
