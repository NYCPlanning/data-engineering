# dcpy.product_metadata

Module for documenting datasets: their versions, descriptions, columns, and generating all related assets (PDFs, spreadsheets, etc.).

## Core Concepts

The product metadata system uses a hierarchical structure with four main layers that define dataset documentation:

### 1. Products
Top-level organizational unit (e.g., `lion`, `factfinder`, `db-pluto`). Each product has a `metadata.yml` file defining:
- Product attributes (display name, description)
- Dataset defaults
- List of datasets

### 2. Datasets
Individual datasets within a product. Each dataset has its own `metadata.yml` defining:
- Attributes (display name, description, each_row_is_a, agency, tags, etc.)
- Columns with descriptions, data types, examples, and value enumerations
- Revisions/change history
- Files and assembly instructions

### 3. Destinations
Where datasets get published (e.g., `open_data_nyc`, `bytes`, `edm`). Defined per-dataset in its metadata.

### 4. Files
Actual data files associated with datasets, including both data files and metadata attachments (readmes, data dictionaries, etc.).

## Override Hierarchy

The system uses **cascading overrides** - settings can be defined at each successive layer:

```
Organization → Product → Dataset → Destination → File
```

Each layer inherits from above but can override specific fields. For example:
- `agency` might be set at the product level for all datasets
- A specific dataset can override it with a different agency
- A destination can further customize attributes for a specific publishing target
- Individual files can have their own overrides

This allows you to set sensible defaults at higher levels while maintaining flexibility for exceptions.

## Key Models

Located in `dcpy.models.product`:

- **`OrgMetadata`** - Root metadata for the entire organization
- **`ProductMetadata`** - Metadata for a single product
- **`DatasetMetadata` (alias: `Metadata`)** - Complete metadata for a dataset
- **`Dataset`** - Core dataset definition with attributes and columns
- **`DatasetColumn`** - Column documentation with type, description, values, etc.
- **`File`** - Dataset file definition
- **`Destination`** - Publishing destination configuration
- **`Package`** - Assembly instructions for bundling files (e.g., into ZIPs)

## Writers

The module includes specialized writers for generating documentation assets:

- **`pdf_writer`** - Generate PDF documentation from metadata
- **`oti_xlsx/`** - Generate Excel-based data dictionaries and abstract documents

These writers consume the metadata models to produce standardized documentation outputs.

## Usage Example

```python
from dcpy.models.product.metadata import OrgMetadata

# Load organization metadata (reads products/metadata.yml)
org_md = OrgMetadata.from_path(repo_path)

# Access a product
product_md = org_md.product("lion")

# Access a dataset within the product
dataset_md = product_md.dataset("pseudo_lots")

# Access dataset attributes
print(dataset_md.attributes.display_name)
print(dataset_md.attributes.description)

# Iterate through columns
for col in dataset_md.columns:
    print(f"{col.id}: {col.name} ({col.data_type})")
```

## File Structure

```
products/
├── metadata.yml              # Organization-level metadata
├── my_product/
│   ├── metadata.yml         # Product-level metadata
│   └── my_dataset/
│       └── metadata.yml     # Dataset-level metadata
```

## Related Documentation

- See `dcpy.models.base` for base serialization classes
- See `dcpy.lifecycle` for how metadata is used in pipeline stages
