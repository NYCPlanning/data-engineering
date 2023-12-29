import pandas as pd
import requests
from bs4 import BeautifulSoup

from . import df_to_tempfile


class Scriptor:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def ingest(self) -> pd.DataFrame:
        lookup = {
            "Casa Office": "1hpyh201fVvv_p6oZKa6IOP8zzeebRy5p",
            "Snap Center": "1GBr0_gE1VBUtJZRLJosLSl1B9Xo",
            "Jobs and Service Center": "1uC3LcicVmGp_CZcyQZkGuf8rLGY",
            "Medicaid Office": "1Ypu_qfcW7jBGDA_2uK1xP3TwUTw",
            "OCSS Center": "1MYy3WOFBNgJ4D4-rqMYoXs2R3dk",
        }

        results = []
        for factype, mid in lookup.items():
            url = f"https://www.google.com/maps/d/kml?mid={mid}&forcekml=1"
            html_content = requests.get(url).content
            soup = BeautifulSoup(html_content, "html.parser")
            descriptions = soup.find_all("description")
            facility = soup.find_all("name")
            data = []
            for d in range(1, len(descriptions)):
                item = descriptions[d].text.replace("\xa0", " ")
                item = item.split("<br>")
                result = {}
                fclty = facility[d + 1].text
                result["factype"] = factype
                for i in range(len(item)):
                    result["facname"] = fclty.replace("\xa0", " ")
                    parse = item[i].split(": ")
                    if len(parse) == 1:
                        pass
                    else:
                        key = parse[0]
                        value = parse[1]
                        if key in ["Location Address", "Address"]:
                            key = "address"
                        if key in ["Zip Code", "Zipcode"]:
                            key = "zipcode"
                        if "hour" in key.lower():
                            key = "hour"
                        result[key] = value
                        data.append(result)
            results += data

        df = pd.DataFrame(results).drop_duplicates().reset_index()
        df = df.drop(columns=["index"])

        return df

    def runner(self) -> str:
        df = self.ingest()
        local_path = df_to_tempfile(df)
        return local_path
