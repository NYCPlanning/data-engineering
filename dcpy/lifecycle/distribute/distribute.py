import typer
from tabulate import tabulate  # type: ignore

from dcpy.models.lifecycle.distribute import DistributeResult

app = typer.Typer()


def _make_results_table(distribute_results: list[DistributeResult]) -> str:
    return tabulate(
        [
            [
                r.dataset_id,
                r.destination_id,
                r.destination_type,
                "✅" if r.success else "❌",
                r.result,
            ]
            for r in distribute_results
        ],
        headers=["dataset", "destination_id", "destination type", "success?", "result"],
        tablefmt="presto",
        maxcolwidths=[20, 10, 10, 8, 70],
    )
