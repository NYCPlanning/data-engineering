from typing import Optional

from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from sqlalchemy import func


class ColumnValuesOfStateProjection(ColumnMapMetricProvider):
    condition_metric_name = "column_values_of_srs"

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):

        return func.ST_SRID(column) == 2263


class ExpectColumnValuesToBeOfStateProjection(ColumnMapExpectation):
    """Expect column values to have a specific spatial reference system ID."""

    map_metric = "column_values_of_srs"
    success_keys = ("mostly",)

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        super().validate_configuration(configuration)
        configuration = configuration or self.configuration


if __name__ == "__main__":
    ExpectColumnValuesToBeOfStateProjection().print_diagnostic_checklist()
