# dcpy Package Structure

Python package for NYC Planning data pipelines. Follow layer rules strictly.

## Layer Architecture

### Layer 1: utils
**Most foundational. Import-safe for all modules.**

- Core utilities used throughout codebase
- Alter with extreme caution
- ALWAYS unit test changes
- No dependencies on other dcpy modules
- Aim for >90% test coverage
### Layer 2: Everything Else (except lifecycle)
**Can import: utils. Should not import each other**

Includes:
- `connectors/` - Data source connections
- `library/` - Shared business logic
- `geosupport/` - Geospatial utilities
- [`product_metadata/`](../../dcpy/product_metadata/README.md) - Product configuration and dataset documentation

Cannot import from `lifecycle`.

### Layer 3: lifecycle
**Can import: everything below. lifecycle stages should avoid importing other stages.**

Entry points for data pipeline stages:
- `ingest/` - Pull data from sources
- `builds/` - Transform data
- `distribute/` - Publish outputs
- `validate/` - Quality checks
- `package/` - Bundle for deployment
- `scripts/` - Cross-stage utilities (prefer this for code used across stages)

**Purpose:** Wire together other parts of the codebase. (e.g. configuring generic connectors for specific purpose, instantiating locally clone metdata, etc.) Avoid heavy business logic here.

## Deprecated

**dcpy.models** - Do not add new code. Move models out when possible.

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
