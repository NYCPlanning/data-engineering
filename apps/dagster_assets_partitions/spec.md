# Dagster Spec

# Hard Rules
- generate only what's required and asked here. Do not generate documentation, like READMEs, extra scripts, etc., without asking me.
- No code comments, unless something might be really confusing. 

## Instructions
- Please consult the documentation on dagster
- Read the documentation on the lifecycle.build in dcpy/lifecycle/builds/README.md
- then ask me clarifying questions
  - I'll update the spec with answers
- Then let's go!

<!-- ## Python Specifics -->
<!-- - DO NOT generate code  -->

## How to Run Code
Please run commands against the docker container called devcontainer. cd into `de/data-engineering`, and run whatever python command is needed.

# Instructions Overview
## Assets Spec
We're going to generate a dagster server with the following assets:

- For ingest: 
  - each template under (project-root)/ingest_templates should have its own asset and partition (a manual string version)
  - these templates should be materialized with dcpy.lifecycle.ingest.run.ingest 

- For builds:
  - Execute the build lifecycle (see lifecycle.builds documentation) for each of our products listed in (data-engineering-root)/products/products.yml
    - each product should have an asset group, created with an asset factory. Iterate over the entries in products/products.yml to generate these.
    - each group should contain an asset for each build stage (see below), namely: plan, load, build, package
    - each lifecycle stage asset should depend on the former stage
  - For now, let's just store assets locally on the machine, under `.lifecycle`
  - Partitions: each product listed should have its own DynamicPartitionsDefinition consisting of a free-text version string, a dash (-), and build-note string. e.g. 2025.1.1-2_ar_build
    - For each: let's generate starter partitions for nightly_qa and main
  - for the builds.build stage, use the build_command defined in products/products.yml 
  - for the builds.package, just stub this out for now

## Code Spec
- Add a requirements.txt with requirements like dagster
- Assume a .venv in the dir: orchestration/dagster/
