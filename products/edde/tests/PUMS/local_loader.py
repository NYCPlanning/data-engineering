from aggregate.PUMS.count_PUMS_demographics import PUMSCountDemographics
from aggregate.PUMS.count_PUMS_economics import PUMSCountEconomics
from aggregate.PUMS.count_PUMS_households import PUMSCountHouseholds
from aggregate.PUMS.median_PUMS_demographics import PUMSMedianDemographics
from aggregate.PUMS.median_PUMS_economics import PUMSMedianEconomics
from ingest.load_data import load_PUMS


class LocalLoader:
    """To persist a dataset between tests. Each testing module has it's own instance
    Possible to-do: return ingestor/aggregator instead of data like load_fraction_aggregator
    """

    def __init__(self, year: int = 2021) -> None:
        self.year = year

    def load_by_person(self, all_data, include_rw=True, variable_set="demographic"):
        """To be called in first test"""
        limited_PUMA = not all_data

        self.ingestor = load_PUMS(
            variable_types=[variable_set],
            limited_PUMA=limited_PUMA,
            include_rw=include_rw,
            return_ingestor=True,
            requery=True,
            year=self.year,
        )
        self.by_person_raw = self.ingestor.vi_data_raw
        self.by_person = self.ingestor.vi_data

    def load_aggregated_counts(self, all_data, type, add_MOE=False, keep_SE=True):
        limited_PUMA = not all_data
        if type == "demographics":
            aggregator = PUMSCountDemographics(
                year=self.year, limited_PUMA=limited_PUMA
            )
        elif type == "economics":
            aggregator = PUMSCountEconomics(
                year=self.year,
                limited_PUMA=limited_PUMA,
                add_MOE=add_MOE,
                keep_SE=keep_SE,
            )
        elif type == "households":
            aggregator = PUMSCountHouseholds(
                year=self.year,
                limited_PUMA=limited_PUMA,
                add_MOE=add_MOE,
                keep_SE=keep_SE,
            )
        self.by_person = aggregator.PUMS
        self.aggregated = aggregator.aggregated

    def load_aggregated_medians(self, all_data, type, add_MOE=False, keep_SE=True):
        limited_PUMA = not all_data
        if type == "demographics":
            aggregator = PUMSMedianDemographics(
                limited_PUMA=limited_PUMA, add_MOE=add_MOE, keep_SE=keep_SE
            )
        elif type == "economics":
            aggregator = PUMSMedianEconomics(
                limited_PUMA=limited_PUMA, add_MOE=add_MOE, keep_SE=keep_SE
            )
        else:
            raise Exception("type must be demographics or economics")

        self.by_person = aggregator.PUMS
        self.aggregated = aggregator.aggregated

    def load_count_aggregator(self, all_data, add_MOE=False, keep_SE=True):
        """How is this different from load_aggregated_counts?"""
        limited_PUMA = not all_data
        self.count_aggregator = PUMSCountDemographics(
            year=self.year, limited_PUMA=limited_PUMA, add_MOE=add_MOE, keep_SE=keep_SE
        )
        self.by_person = self.count_aggregator.PUMS
        self.aggregated = self.count_aggregator.aggregated
