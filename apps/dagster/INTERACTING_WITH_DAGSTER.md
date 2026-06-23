# Interacting with the Running Dagster Server

This document provides instructions for how to trigger asset materializations and interact with a running Dagster server programmatically.

## Prerequisites

- Dagster server must be running (typically at `http://localhost:3000`)
- Working directory should be `/Users/alexrichey/dev/data-engineering-de2/apps/dagster`
- Python environment with Dagster installed

## Method 1: Using Python Script (Recommended)

The most reliable way to trigger builds is to use a Python script that directly calls the Dagster API.

### Example: Trigger EDDE Build

```python
#!/usr/bin/env python3
import sys
from pathlib import Path

# Add paths
sys.path.insert(0, "/Users/alexrichey/dev/data-engineering-de2/apps/dagster")
sys.path.insert(0, "/Users/alexrichey/dev/data-engineering-de2")

from dagster import DagsterInstance
from lifecycle_repo import defs

# Get instance
instance = DagsterInstance.get()

# Find the asset
build_edde = None
for asset_spec in defs.get_all_asset_specs():
    if asset_spec.key.path == ["build_edde"]:
        build_edde = asset_spec
        break

if not build_edde:
    print("ERROR: Could not find build_edde asset")
    sys.exit(1)

# Trigger materialization with partition
partition_key = "2026:ar_edde:20260623T1809"
print(f"Triggering build_edde with partition: {partition_key}")

# Use the Dagster instance to submit a run
from dagster import ​RunRequest, external_assets_from_specs

# Get the job
job = defs.get_job_def("__ASSET_JOB")

# Execute
result = job.execute_in_process(
    instance=instance,
    asset_selection=[build_edde.key],
    partition_key=partition_key,
)

print(f"Result: {result.success}")
```

## Method 2: Using GraphQL API (Alternative)

The Dagster server exposes a GraphQL API at `http://localhost:3000/graphql`.

### Issues with GraphQL

- The GraphQL API requires exact parameters including `jobName`, `repositoryLocationName`, `repositoryName`
- Partition keys must be passed as tags in `executionMetadata`
- The API is finicky and error messages aren't always clear

### Example GraphQL Mutation

```bash
curl -X POST http://localhost:3000/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation LaunchRun($executionParams: ExecutionParams!) { launchRun(executionParams: $executionParams) { __typename ... on LaunchRunSuccess { run { id runId status } } ... on PythonError { message stack } } }",
    "variables": {
      "executionParams": {
        "selector": {
          "repositoryLocationName": "lifecycle_repo.py",
          "repositoryName": "__repository__",
          "jobName": "__ASSET_JOB",
          "assetSelection": [{"path": ["build_edde"]}]
        },
        "executionMetadata": {
          "tags": [{"key": "dagster/partition", "value": "2026:ar_edde:20260623T1809"}]
        }
      }
    }
  }'
```

**Problems with this approach:**
- Often returns `InvalidSubsetError` or parameter validation errors
- Requires knowing exact repository structure
- Not recommended for automated use

## Method 3: Using Dagster CLI (Not Recommended)

The Dagster CLI has issues when trying to materialize assets:

```bash
# This doesn't work reliably
python -m dagster asset materialize --select build_edde --partition "2026:ar_edde:20260623T1809" -f lifecycle_repo.py
```

**Problems:**
- Requires correct file path resolution
- Working directory matters
- Module import issues

## Method 4: Direct Build Module Call (For Testing)

For testing the build process directly without Dagster:

```python
#!/usr/bin/env python3
import sys
from pathlib import Path

sys.path.insert(0, "/Users/alexrichey/dev/data-engineering-de2")

from dcpy.lifecycle.builds import build as build_module

product_path = Path("/Users/alexrichey/dev/data-engineering-de2/products/edde")
build_directory = Path("/Users/alexrichey/dev/.de_lifecycle_data/builds/build/edde/26v1")

# Recipe lock file should be in build_directory/recipe.lock.yml
output_path = build_module.build(
    product_path=product_path,
    build_directory=build_directory,
)

print(f"Build completed: {output_path}")
```

## Checking Build Status

### Check Build Directories

```bash
# Check if build outputs exist
ls -la /Users/alexrichey/dev/.de_lifecycle_data/builds/build/edde/

# Check specific version directory
ls -la /Users/alexrichey/dev/.de_lifecycle_data/builds/build/edde/2026/

# Check package outputs
find /Users/alexrichey/dev/.de_lifecycle_data/builds/build/edde/2026/package -type f | head -20
```

### Check Dagster Run Logs

```bash
# Find recent runs
ls -lt /Users/alexrichey/dev/.de_lifecycle_data/apps/dagster/storage/*/runs/ | head -10

# Check specific run database
sqlite3 /Users/alexrichey/dev/.de_lifecycle_data/apps/dagster/storage/runs/<run_id>.db ".tables"
```

## Common Issues

### 1. Module Import Errors

**Problem:** `ModuleNotFoundError: No module named 'lifecycle_repo'`

**Solution:** Ensure working directory is `/Users/alexrichey/dev/data-engineering-de2/apps/dagster`

### 2. Wrong Directory for Outputs

**Problem:** Outputs going to `2026-06-01` instead of `2026`

**Solution:** Ensure `recipe.version` is set correctly (not `BUILD_ENV_EDDE_VERSION`)

### 3. GraphQL InvalidSubsetError

**Problem:** GraphQL API returns `InvalidSubsetError`

**Solution:** Use Python script method instead of GraphQL

## Best Practice for Agents

When automating Dagster interactions:

1. **Always use Python scripts** that directly import and call Dagster modules
2. **Set working directory** to `/Users/alexrichey/dev/data-engineering-de2/apps/dagster`
3. **Add both paths** to sys.path:
   - `/Users/alexrichey/dev/data-engineering-de2/apps/dagster`
   - `/Users/alexrichey/dev/data-engineering-de2`
4. **Check build outputs** by examining the filesystem, not just Dagster API responses
5. **Use partition keys** in format: `{version}:{timestamp}:{branch}`

## Example Complete Workflow

```python
#!/usr/bin/env python3
"""Complete workflow for triggering and verifying EDDE build."""

import sys
import time
from pathlib import Path

# Setup paths
sys.path.insert(0, "/Users/alexrichey/dev/data-engineering-de2/apps/dagster")
sys.path.insert(0, "/Users/alexrichey/dev/data-engineering-de2")

# 1. Trigger build (use appropriate method from above)
print("Triggering build...")
# ... trigger code ...

# 2. Wait for build to complete
print("Waiting for build...")
time.sleep(120)  # Adjust based on expected build time

# 3. Verify outputs exist
output_dir = Path("/Users/alexrichey/dev/.de_lifecycle_data/builds/build/edde/2026")
data_dir = output_dir / "data"
package_dir = output_dir / "package"

print("\nVerifying outputs:")
print(f"  Data directory exists: {data_dir.exists()}")
print(f"  Package directory exists: {package_dir.exists()}")

if package_dir.exists():
    subdirs = list(package_dir.iterdir())
    print(f"  Package subdirectories: {[d.name for d in subdirs if d.is_dir()]}")

# 4. Check for wrong directory
wrong_dir = Path("/Users/alexrichey/dev/.de_lifecycle_data/builds/build/edde/2026-06-01")
if wrong_dir.exists():
    print(f"\n⚠️  WARNING: Outputs in wrong directory: {wrong_dir}")
else:
    print(f"\n✓ No outputs in wrong directory")
```
