from aggregate.PUMS.aggregate_medians import PUMSMedians


class PUMSMedianDemographics(PUMSMedians):
    """Crosstabs on idicators work differently for this aggregator.
    Instead of combining crosstab and original indicator into one, crosstabs are
    included as iterable. Indicators list has elements of (indicator, iterable of crosstabs)
    """

    indicators_denom = [("age",)]
    crosstabs = ["race"]

    def __init__(
        self,
        year: int,
        limited_PUMA=False,
        requery=False,
        add_MOE=True,
        keep_SE=False,
        geo_col="puma",
    ) -> None:
        self.add_MOE = add_MOE
        self.keep_SE = keep_SE
        self.EDDT_category = "demographics"
        self.categories = {}
        PUMSMedians.__init__(
            self,
            year=year,
            variable_types=["demographics"],
            limited_PUMA=limited_PUMA,
            requery=requery,
            add_MOE=add_MOE,
            keep_SE=keep_SE,
            geo_col=geo_col,
        )

    def age_assign(self, person):
        return person["AGEP"]
