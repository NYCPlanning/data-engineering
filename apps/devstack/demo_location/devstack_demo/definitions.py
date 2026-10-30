"""plan (small) -> build (standard) -> publish (esri), per borough, each run on a
dagster-broker worker via Pipes. gpu_train needs a 'gpu' worker the devstack doesn't have,
so it stays queued and shows up in /metrics/demand?caps=gpu.

Dagster never touches the data or holds gateway credentials: steps write to a staging
prefix and the worker promotes it to ``demo/<asset>/<partition>/``.
"""

import os

import dagster as dg

from dagster_broker.pipes_dagster import PipesBrokerClient

BUCKET = os.environ.get("DEVSTACK_BUCKET", "devstack")
PARTITIONS = dg.StaticPartitionsDefinition(["bk", "bx", "mn", "qn", "si"])
STEP = ["python", "-m", "devstack_demo.steps"]


def prefix(asset: str, partition: str) -> str:
    return f"demo/{asset}/{partition}/"


class BuildConfig(dg.Config):
    files: int = 3
    file_kb: int = 64
    sleep_seconds: float = 2.0


@dg.asset(partitions_def=PARTITIONS, kinds={"dagster-broker"})
def plan(context: dg.AssetExecutionContext, broker: PipesBrokerClient):
    p = context.partition_key
    return broker.run(
        context=context,
        command=[*STEP, "plan"],
        required_caps=["small"],
        outputs_prefix=prefix("plan", p),
    ).get_materialize_result()


@dg.asset(partitions_def=PARTITIONS, deps=[plan], kinds={"dagster-broker"})
def build(
    context: dg.AssetExecutionContext,
    config: BuildConfig,
    broker: PipesBrokerClient,
):
    p = context.partition_key
    return broker.run(
        context=context,
        command=[*STEP, "build"],
        required_caps=["standard"],
        extras={"inputs": {"plan": prefix("plan", p)}, **config.model_dump()},
        outputs_prefix=prefix("build", p),
    ).get_materialize_result()


@dg.asset(
    partitions_def=PARTITIONS,
    deps=[build],
    kinds={"dagster-broker"},
)
def publish(context: dg.AssetExecutionContext, broker: PipesBrokerClient):
    p = context.partition_key
    return broker.run(
        context=context,
        command=[*STEP, "publish"],
        required_caps=["esri"],
        extras={"inputs": {"build": prefix("build", p)}},
        outputs_prefix=prefix("publish", p),
    ).get_materialize_result()


@dg.asset(
    partitions_def=PARTITIONS,
    deps=[build],
    kinds={"dagster-broker"},
)
def gpu_train(context: dg.AssetExecutionContext, broker: PipesBrokerClient):
    p = context.partition_key
    return broker.run(
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
    resources={"broker": PipesBrokerClient(bucket=BUCKET)},
)
