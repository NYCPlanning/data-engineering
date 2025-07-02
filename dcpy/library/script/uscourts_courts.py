import pandas as pd

from . import get_json_content, df_to_tempfile


class Scriptor:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.sites = {
            "NY": "https://www.uscourts.gov/fedcf-query?query={%22by%22:%22location%22,%22page%22:0,%22description%22:%22New%20York,%20NY,%20USA%22,%22county%22:%22New%20York%22,%22state%22:%22NY%22,%22zip%22:%2210007%22,%22country%22:%22US%22,%22locationType%22:%22other%22,%22lat%22:40.7127503,%22lng%22:-74.00597649999997,%22filters%22:%22default%22}",
            "BK": "https://www.uscourts.gov/fedcf-query?query={%22by%22:%22location%22,%22page%22:0,%22description%22:%22Brooklyn,%20NY,%20USA%22,%22county%22:%22Kings%22,%22state%22:%22NY%22,%22zip%22:%2211216%22,%22country%22:%22US%22,%22locationType%22:%22other%22,%22lat%22:40.6781281,%22lng%22:-73.94416899999999,%22filters%22:%22default%22}",
        }

    def get_location(self, boro):
        content = get_json_content(self.sites.get(boro))
        locations = content.get("results").get("locations")
        return locations

    # deduplicate, --> in reality there's no duplicated items
    def removeduplicate(self, it):
        seen = []
        for x in it:
            t = tuple(x.items())
            if t not in seen:
                yield x
                seen.append(t)

    def ingest(self) -> pd.DataFrame:
        data = self.get_location("NY") + self.get_location("BK")
        data = list(self.removeduplicate(data))
        ## mypy was unhappy. moot point - these urls are dead. See #1326
        df = pd.DataFrame.from_dict(data, orient="columns")  # type: ignore
        return df

    def runner(self) -> str:
        df = self.ingest()
        local_path = df_to_tempfile(df)
        return local_path
