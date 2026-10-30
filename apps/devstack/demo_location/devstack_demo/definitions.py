"""plan -> build -> publish, per borough, each step run on a dispatch worker via Pipes.
Plus gpu_train, which needs a 'gpu' worker that the devstack doesn't have: it stays queued
and shows up in dispatch's /metrics/demand?caps=gpu.

The Dagster side never touches the step's data: steps read upstream outputs from, and write
their own to, the S3 gateway; PipesDispatchClient promotes staged outputs to
``demo/<asset>/<partition>/`` once dispatch reports the item done.
"""

import os

import dagster as dg

from dispatch.pipes_dagster import PipesDispatchClient

BUCKET = os.environ.get("DEVSTACK_BUCKET", "devstack")
PARTITIONS = dg.StaticPartitionsDefinition(["bk", "bx", "mn", "qn", "si"])
STEP = ["python", "-m", "devstack_demo.steps"]


def prefix(asset: str, partition: str) -> str:
    return f"demo/{asset}/{partition}/"


class BuildConfig(dg.Config):
    files: int = 3
    file_kb: int = 64
    sleep_seconds: float = 2.0


@dg.asset(partitions_def=PARTITIONS, kinds={"dispatch"})
def plan(context: dg.AssetExecutionContext, dispatch: PipesDispatchClient):
    p = context.partition_key
    return dispatch.run(
        context=context,
        command=[*STEP, "plan"],
        outputs_prefix=prefix("plan", p),
    ).get_materialize_result()


@dg.asset(partitions_def=PARTITIONS, deps=[plan], kinds={"dispatch"})
def build(
    context: dg.AssetExecutionContext,
    config: BuildConfig,
    dispatch: PipesDispatchClient,
):
    p = context.partition_key
    return dispatch.run(
        context=context,
        command=[*STEP, "build"],
        extras={"inputs": {"plan": prefix("plan", p)}, **config.model_dump()},
        outputs_prefix=prefix("build", p),
    ).get_materialize_result()


@dg.asset(
    partitions_def=PARTITIONS, deps=[build], kinds={"dispatch"}, tags={"caps": "esri"}
)
def publish(context: dg.AssetExecutionContext, dispatch: PipesDispatchClient):
    p = context.partition_key
    return dispatch.run(
        context=context,
        command=[*STEP, "publish"],
        required_caps=["esri"],
        extras={"inputs": {"build": prefix("build", p)}},
        outputs_prefix=prefix("publish", p),
    ).get_materialize_result()


@dg.asset(
    partitions_def=PARTITIONS, deps=[build], kinds={"dispatch"}, tags={"caps": "gpu"}
)
def gpu_train(context: dg.AssetExecutionContext, dispatch: PipesDispatchClient):
    p = context.partition_key
    return dispatch.run(
        context=context,
        command=[*STEP, "plan"],
        required_caps=["gpu"],
        outputs_prefix=prefix("gpu_train", p),
    ).get_materialize_result()


demo_chain = dg.define_asset_job(
    "demo_chain", selection=[plan, build, publish], partitions_def=PARTITIONS
)
gpu_job = dg.define_asset_job(
    "gpu_job", selection=[gpu_train], partitions_def=PARTITIONS
)

defs = dg.Definitions(
    assets=[plan, build, publish, gpu_train],
    jobs=[demo_chain, gpu_job],
    resources={"dispatch": PipesDispatchClient(bucket=BUCKET, poll_interval=1.0)},
)
