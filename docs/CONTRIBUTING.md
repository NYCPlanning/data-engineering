# Contributing

Terse reference for agents working in this codebase.

## By Skill Area

### Python
- [dcpy package structure & conventions](./dcpy/README.md)

### dbt
- [dbt project conventions](./dbt/project_conventions.md)
- **If adding a dbt model, read the conventions doc above first**

### Frontend/Products
- [apps, glossary, and environment setup](./products/README.md)

### Scripts & CLI Tools
- [Available bash scripts and utilities](./bash/SCRIPTS.md)

## General Rules
- Follow existing patterns in the codebase
- Review the Scripts and CLI Tools to see the tools at your disposal
- Test changes before committing
- Use direnv for local development (see products docs) but do not implement in product scripts or dcpy. Prod runs without direnv.

## Testing

### Product Metadata Tests

Tests in `dcpy/test/product_metadata/` require the real product-metadata repository:

1. **Clone the product-metadata repo:**
   ```bash
   git clone https://github.com/NYCPlanning/product-metadata.git ~/product-metadata
   ```

2. **Set the environment variable:**
   ```bash
   export PRODUCT_METADATA_REPO_PATH=~/product-metadata
   ```

   Add this to your shell configuration file (`.bashrc`, `.zshrc`, etc.) to make it permanent.

3. **Run the tests:**
   ```bash
   pytest dcpy/test/product_metadata/
   ```

The product metadata tests validate:
- Metadata loading and validation
- Override hierarchy (org → product → dataset → destination)
- Template variable substitution from `snippets/strings.yml`
- Destination querying and filtering
- File and column overrides
