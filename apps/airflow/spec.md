# Airflow Spec

# Hard Rules
- generate only what's required and asked here. Do not generate documentation, like READMEs, extra scripts, etc., without asking me.
- No code comments, unless something might be really confusing. 

## Instructions
- Please consult the documentation on airflow
- Read the documentation on the lifecycle.build in dcpy/lifecycle/builds/README.md
- Read the "Assets spec" section here
- Read through the code in `dagster_assets_partitions` as a reference to how this was done in dagster. We want to take a similar approach in terms of the "assets" generated but many concepts in dagster and airflow are not one-to-one.
- then ask me clarifying questions
  - I'll update the spec with answers
- Then let's go!

<!-- ## Python Specifics -->
<!-- - DO NOT generate code  -->

## How to Run Code
Please run commands using python in .venv (in the root of this project)

# Instructions Overview
## Assets Spec

We're going to generate a airflow server with the following assets:

- For ingest: 
  - each template under (project-root)/ingest_templates should have its own asset and partition (a manual string version)
  - these templates should be materialized with dcpy.lifecycle.ingest.run.ingest

- For builds:
  - Execute the build lifecycle (see lifecycle.builds documentation) for each of our products listed in (data-engineering-root)/products/products.yml
    - each product should have a declared dag, ideally created wiht some sort of factory. Iterate over the entries in products/products.yml to generate these.
    - the output should be an airflow asset
    - each group should contain an task for each build stage (see below), namely: plan, load, build, package
    - each lifecycle stage task should depend on the former stage
  - For now, let's just store assets/task outputs locally on the machine, under `.lifecycle`
  - for the builds.build stage, use the build_command defined in products/products.yml 
  - for the builds.package, just stub this out for now

## Other notes
- let's use decorators in general over `with DAG(...):`
- Partitions: dagster has a concept of partitions, basically labels that version the data. How might this be handled in airflow?

### Q&A

**Q1: Partitions in Airflow**
In Airflow, the equivalent to Dagster's dynamic partitions would be task instances with different logical dates/run dates. Should I use:
- `DynamicTaskGroup` with dynamic task mapping for ingest templates and products?
- Or create explicit partitions using `@task_group` decorator with parameters passed through XCom?
- For versioning the data (like "ingest_version"), should each ingest template have an operator parameter for version selection?

A: To the first question, let's try the first option. For ingest, an operator parameter makes sense to me.

**Q2: Asset vs DAG/Task structure**
Should each ingest template and build product be:
- Its own independent task group with separate dependencies?
- Or grouped within larger DAGs (one for all ingests, one for all builds)?

A: for now, independent

**Q3: Local storage path**
For `.lifecycle` outputs, should I:
- Create a consistent path structure like `.lifecycle/ingest/{template_id}/{version}/` and `.lifecycle/builds/{product_id}/{partition_key}/`?
- Store these paths as Airflow variables or just hardcode them?

A: hardcode for now

**Q4: Airflow context and configuration**
- What Python version and Airflow version should I target? (This affects decorator availability)
- Should I use a custom Airflow Provider or just standard operators?

A: python 3.13, latest version of airflow. Standard operators
