import prefect

# from prefect.flow_runs import pause_flow_run
from dcpy.utils.logging import logger
import asyncio


# Tasks
# Tasks are bite-sized tasks with some transactional semantics.
# They have built-in concurrency/parallelization (as opposed to flows).
#
# Tasks can invoke other tasks. They cannot invoke Flows.
# All tasks within a flow run on the same machine
@prefect.task
def get_destinations():
    return ["a", "b", "c"]


@prefect.task(task_run_name="push_dataset:{dest}")
def push_dataset(dest: str):
    return f"pushed to {dest}"


@prefect.task(task_run_name="publish_revision:{dest}")
def publish_revision(dest: str):
    return f"published {dest}"


# Flows
# Flows are a level above tasks. They may invoke other flows (subflows) or tasks.
# They may be spread across any available workers.
# In terms of async/parallelization, you have to engineer that yourself, via processes, asyncio, etc.
@prefect.flow(flow_run_name="package:{dest}")
async def package(dest: str):
    return f"packaged {dest}"


@prefect.flow(flow_run_name="distribute:{dest}")
async def distribute(dest: str):
    push_dataset.submit(dest).wait()
    # should_publish = await pause_flow_run(wait_for_input=str)
    # if should_publish:
    #     publish_revision.submit(dest).wait()
    return f"distributed {dest}"


@prefect.flow(flow_run_name="package_and_distribute:{dest}")
async def package_and_distribute(dest: str):
    await package(dest)
    await distribute(dest)
    return f"distributed {dest}"


@prefect.flow(flow_run_name="run:{greeting}")
async def run(greeting: str):
    # Note: neither of these are showing up under the logs in prefect. Some extra config
    # is required to hook our existing logger up to the prefect logger.
    print(greeting)
    logger.info(greeting)

    dests = get_destinations.submit().result()
    task_futures = [package_and_distribute(dest) for dest in dests]
    results = await asyncio.gather(*task_futures)
    return results


if __name__ == "__main__":
    # To create a deployment on the prefect server. This allows you to trigger jobs from the UI
    run.serve(name="my-first-deployment")

    # To run the flow locally, which will still register on the dashboard:
    # asyncio.run(run())
