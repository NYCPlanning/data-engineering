# Contributing

Terse reference for agents working in this codebase.

## By Skill Area

### Python
- [dcpy package structure & conventions](./dcpy/README.md)

### dbt
- [dbt project conventions](./dbt/project_conventions.md)

### Frontend/Products
- [apps, glossary, and environment setup](./products/README.md)

### Scripts & CLI Tools
- [Available bash scripts and utilities](./bash/SCRIPTS.md)

## General Rules
- Follow existing patterns in the codebase
- Review the Scripts and CLI Tools to see the tools at your disposal
- Test changes before committing
- Use direnv for local development (see products docs) but do not implement in product scripts or dcpy. Prod runs without direnv.
