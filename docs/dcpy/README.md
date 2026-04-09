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
