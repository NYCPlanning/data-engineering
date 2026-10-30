"""Runner smoke tests: one job per routing case, each running
dagster_broker.test_steps.work on a dagster-broker worker via Pipes. No worker has
'gpu', so the gpu job stays queued until its run is terminated."""

import os

import dagster as dg
from dagster_utils.pipes_broker import PipesBrokerClient


class StepConfig(dg.Config):
    seconds: float = 1.0
    mb: int = 1
    parts: int = 1
    fail: bool = False


# name: (required caps, step config)
CASES = {
    "small": (["small"], {}),
    "standard": (["standard"], {"seconds": 3, "parts": 3}),
    "esri": (["esri"], {}),
    "gpu": (["gpu"], {}),
    "failing": (["small"], {"fail": True}),
    "slow": (["standard"], {"seconds": 60, "parts": 3}),
    "write": (["standard"], {"mb": 64, "parts": 4}),
}


def case(name: str, caps: list[str]):
    @dg.asset(name=name, group_name="runner_tests", kinds={"dagster-broker"})
    def step(
        context: dg.AssetExecutionContext,
        config: StepConfig,
        broker: PipesBrokerClient,
    ):
        return broker.run(
            context=context,
            function="dagster_broker.test_steps.work",
            args=config.model_dump(),
            required_caps=caps,
            outputs_prefix=f"runner_tests/{name}/{context.run.run_id}/",
            retry_on_failure=not config.fail,
        ).get_materialize_result()

    return step


assets = [case(name, caps) for name, (caps, _) in CASES.items()]
jobs = [
    dg.define_asset_job(
        f"{name}_job", [name], config={"ops": {name: {"config": config}}}
    )
    for name, (_, config) in CASES.items()
]

defs = dg.Definitions(
    assets=assets,
    jobs=jobs,
    resources={
        "broker": PipesBrokerClient(
            bucket=os.environ.get("DAGSTER_BROKER_BUCKET", "dagster-broker")
        )
    },
)
