"""Examine median wages broken out by industry, occupation"""

import pandas as pd
from aggregate.PUMS.aggregate_PUMS import PUMSAggregator
from aggregate.PUMS.economic_indicators import (
    industry_assign,
    lf_assign,
    occupation_assign,
)
from ingest.load_data import load_PUMS
from statistical.calculate_medians import (
    calculate_median_with_crosstab,
)


class PUMSMedianEconomics(PUMSAggregator):
    """Because this has a double crosstab on race and industry/occupation it needs it's own
    implementation of calculate_add_new_variable and won't use PUMSMedian's
    Note: bad desing to add self.PUMS here, should only be set in parent PUMS aggregator
    This code involves some hot fixes to hit deadline, call can be addressed with refactor
    to do aggregatation in call of PUMSAggregator instead of init"""

    indicators_denom = [("wage", "civilian_employed_with_earnings_filter")]
    economic_crosstabs = [
        "industry",
        # "occupation"
    ]
    crosstabs = ["race"]

    def __init__(
        self,
        year,
        limited_PUMA=False,
        requery=False,
        add_MOE=True,
        keep_SE=False,
        geo_col="puma",
    ):
        self.PUMS: pd.DataFrame = load_PUMS(
            year=year,
            variable_types=["demographics", "economics"],
            limited_PUMA=limited_PUMA,
            requery=requery,
        )
        self.add_MOE = add_MOE
        self.keep_SE = keep_SE
        self.geo_col = geo_col
        self.categories = {}
        self.EDDT_category = "economics"
        self.calculation_type = "medians"
        for economic_crosstab in self.economic_crosstabs:
            """Unusual to assign indicator without aggregating on it directly.
            Using column as crosstab instead of indicator to report"""
            self.assign_indicator(economic_crosstab)
            self.add_category(economic_crosstab)
        PUMSAggregator.__init__(
            self,
            variable_types=["demographics", "economics"],
            limited_PUMA=limited_PUMA,
            year=year,
            requery=requery,
            PUMS=self.PUMS,
            geo_col=self.geo_col,
        )

    def assign_indicator(self, indicator) -> pd.DataFrame:
        """Copy/pasted from PUMS aggregator class. Can't call version from parent class
        as the aggregations are done on init and this needs to be run before init.
        Better design is not to do aggregation on init of parent class but rather in call.
        For now use hot fix to get around this"""
        if indicator not in self.PUMS.columns:
            self.PUMS[indicator] = self.PUMS.apply(
                axis=1, func=self.__getattribute__(f"{indicator}_assign")
            )

    def wage_assign(self, person):
        return person["WAGP"]

    def lf_assign(self, person):
        return lf_assign(person)

    def occupation_assign(self, person):
        return occupation_assign(person)

    def industry_assign(self, person):
        return industry_assign(person)

    def civilian_employed_with_earnings_filter(self, PUMS: pd.DataFrame):
        age_subset = PUMS[(PUMS["AGEP"] >= 16) & (PUMS["AGEP"] <= 64)]
        civilian_subset = age_subset[
            age_subset["ESR"].isin(
                [
                    "Civilian employed, with a job but not at work",
                    "Civilian employed, at work",
                ]
            )
        ]
        with_earnings_subset = civilian_subset[civilian_subset["WAGP"] > 0]
        return with_earnings_subset

    def calculate_add_new_variable(self, ind_denom):
        """Overwrites from parent class. Specific to economic medians as this is the
        median that is broken out by two crosstabs: race and industry/occupation"""
        indicator = ind_denom[0]
        self.assign_indicator(indicator)
        subset = self.apply_denominator(ind_denom)
        for crosstab in self.economic_crosstabs:
            self.assign_indicator(crosstab)
            new_indicator = calculate_median_with_crosstab(
                data=subset,
                variable_col=indicator,
                crosstab_col=crosstab,
                rw_cols=self.rw_cols,
                weight_col=self.weight_col,
                geo_col=self.geo_col,
                add_MOE=self.add_MOE,
                keep_SE=self.keep_SE,
            )
            self.add_aggregated_data(new_indicator)

            for race in self.categories["race"]:
                records_in_race = subset[subset["race"] == race]
                if not records_in_race.empty:
                    new_indicator = calculate_median_with_crosstab(
                        data=records_in_race,
                        variable_col=indicator,
                        crosstab_col=crosstab,
                        rw_cols=self.rw_cols,
                        weight_col=self.weight_col,
                        geo_col=self.geo_col,
                        add_MOE=self.add_MOE,
                        keep_SE=self.keep_SE,
                        second_crosstab_name=race,
                    )
                    self.add_aggregated_data(new_indicator)

    def order_columns(self):
        """Overwrites method in PUMSAggregator as this class does double crosstab thing.
        General medians class will also need custom version"""

        col_order = []
        for ind in self.economic_crosstabs:
            for ind_category in self.categories[ind]:
                for measure in ["median"]:
                    col_order.append(f"{ind_category}-wage-{measure}")
                    col_order.append(f"{ind_category}-wage-{measure}-cv")
                    col_order.append(f"{ind_category}-wage-{measure}-moe")
            if not self.household:
                for ind_category in self.categories[ind]:
                    for race_crosstab in self.categories["race"]:
                        for measure in ["median"]:
                            column_label_base = (
                                f"{ind_category}-wage-{race_crosstab}-{measure}"
                            )
                            col_order.append(f"{column_label_base}")
                            col_order.append(f"{column_label_base}-cv")
                            col_order.append(f"{column_label_base}-moe")
        self.aggregated = self.aggregated.reindex(columns=col_order)
