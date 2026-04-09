# Products (Svelte Apps)

Frontend applications built with SvelteKit.

## Glossary

**product** - A collection of datasets (e.g. LION or PLUTO) potentially with pipelines under `products` folder in the root.

**dataset** - Roughly, data that can be turned into a table or other single output (e.g. exported to a shapefile, a binary, etc)

**dataset file** - A file version of a dataset

**recipe** - Configuration defining how to build a dataset (sources, transforms, outputs)

**ingest_template** - Reusable configuration for pulling data from common sources

## Environment Setup

Each product has `.envrc` files for local development.

**Load direnv before running commands in products/*:**
```bash
eval "$(direnv export bash)" && <your-command>
# or
source load_direnv.sh && <your-command>
```

Critical variables like `BUILD_ENGINE_SCHEMA` are set here.

**⚠️ NEVER include direnv logic in production code outputs**

## Related Docs

- [dbt project conventions](../dbt/project_conventions.md) - Data modeling standards
