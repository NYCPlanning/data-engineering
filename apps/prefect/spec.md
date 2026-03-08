# Overview

Create a POC of prefect.

- Docker Compose, with postgres, redis, a prefect server, and a worker. (and maybe minio) 
- Let's implement a mocked version of our dataset lifecycle for builds

# Hard Rules
- No code comments, unless something might be really confusing. 

## Instructions
- Please consult the documentation on prefect
- Read the documentation on the lifecycle.build in dcpy/lifecycle/builds/README.md
- then ask me clarifying questions
  - I'll update the spec with answers
- Then let's go!

# Instructions Overview
##  Spec
We're going to generate a prefect server with the following functionality for builds:

Execute the build lifecycle (see lifecycle.builds documentation). Each lifecycle stage should depend on the former stage. We can use either local storage, or minio, whichever is easiest


# Questions
1. Docker Compose Setup: Do you want the Prefect server and worker to run in separate containers, or would you prefer a single Prefect container that can handle both server and worker functionality?
- separate containers would be nice. 

2. Database Storage: Should the Prefect server use the same PostgreSQL database as the builds, or would you prefer a separate database for Prefect metadata?
- for mocked builds, let's not have them use the db. So in the compose, only prefect would touch the db.

3. Build Execution: The lifecycle stages are currently CLI-based (plan.py, load.py, build.py, etc.). Should the Prefect flows:
  Call these CLI commands directly via subprocess?
  Import and call the Python functions directly?
  Create new Prefect-native implementations?
- Import and call the Python functions directly. 

4. Data Storage: You mentioned "local storage or minio" - should I include both options, or do you have a preference? Minio would be more production-like.
- let's use Minio

5. Mock Implementation: When you say "mocked version", do you mean:
  Use fake/dummy data instead of real datasets?
  Skip actual data processing and just simulate the workflow stages?
  Use a simplified version of the real lifecycle logic?
- Skip actual data processing and just simulate the workflow stages

6. Flow Dependencies: Should each lifecycle stage (plan → load → build → package) be:
  Separate Prefect flows with dependencies between them?
  Tasks within a single comprehensive flow?
  Subflows within a parent flow?
- Let's chat about this one. 

7. Configuration: Should the Prefect flows read from the same recipe.yml files that the current system uses, or should we create simplified configuration for the POC?
- let's use a simplified, pared down recipe. Let's just have it pull in the bpl_libraries dataset.