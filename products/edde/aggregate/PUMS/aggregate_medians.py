"""Between PUMS aggregator and base classes"""
from distutils.util import subst_vars
from aggregate.PUMS.aggregate_PUMS import PUMSAggregator
from statistical.calculate_medians_LI import calculate_median_LI


class PUMSMedians(PUMSAggregator):
    """Crosstabs on idicators work differently for this aggregator.
    Instead of combining crosstab and original indicator into one, crosstabs are
    included as iterable. Indicators list has elements of (indicator, iterable of crosstabs)
    """

    def __init__(
        self,
        year: int,
        variable_types,
        geo_col,
        limited_PUMA=False,
        requery=False,
        add_MOE=True,
        keep_SE=False,
    ) -> None:
        self.add_MOE = add_MOE
        self.keep_SE = keep_SE
        self.calculation_type = "medians"
        PUMSAggregator.__init__(
            self,
            year=year,
            variable_types=variable_types,
            limited_PUMA=limited_PUMA,
            requery=requery,
            geo_col=geo_col,
            order_columns=False,
            include_rw=False,
        )

    def calculate_add_new_variable(self, ind_denom):
        """Overwrites from parent class"""
        indicator = ind_denom[0]
        self.assign_indicator(indicator)
        subset = self.apply_denominator(ind_denom)

        new_indicator_aggregated = calculate_median_LI(
            data=subset.copy(),
            variable_col=indicator,
            geo_col=self.geo_col,
        )
        self.add_aggregated_data(new_indicator_aggregated)

        for race in self.categories["race"]:
            records_in_race = subset[subset["race"] == race]
            new_col_label = f"{indicator}_{race}"
            if not records_in_race.empty:
                median_aggregated_crosstab = calculate_median_LI(
                    data=records_in_race.copy(),
                    variable_col=indicator,
                    geo_col=self.geo_col,
                    new_col_label=new_col_label,
                )
            self.add_aggregated_data(median_aggregated_crosstab)
