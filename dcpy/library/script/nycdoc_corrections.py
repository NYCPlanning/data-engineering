import re
import ssl
from urllib.request import Request, urlopen

import pandas as pd
import usaddress
from bs4 import BeautifulSoup

from . import df_to_tempfile
from .scriptor import ScriptorInterface


class Scriptor(ScriptorInterface):
    def get_hnum(self, address):
        result = [k for (k, v) in usaddress.parse(address) if re.search("Address", v)]
        return " ".join(result)

    def get_sname(self, address):
        result = [k for (k, v) in usaddress.parse(address) if re.search("Street", v)]
        return " ".join(result)

    def get_zipcode(self, address):
        result = [k for (k, v) in usaddress.parse(address) if re.search("ZipCode", v)]
        return " ".join(result)

    def ingest(self) -> pd.DataFrame:
        hdr = {"User-Agent": "Mozilla/5.0"}
        req = Request(self.path, headers=hdr)
        gcontext = ssl.SSLContext()
        page = urlopen(req, context=gcontext)
        soup = BeautifulSoup(page, features="html.parser")
        data = []
        targets = [
            "Anna M. Kross Center (AMKC)",
            "Eric M. Taylor Center (EMTC) Formerly known as CIFM",
            "George Motchan Detention Center (GMDC)",
            "George R. Vierno Center (GRVC)",
            "James A. Thomas Center (JATC)",
            "North Infirmary Command (NIC)",
            "Otis Bantum Correctional Center (OBCC)",
            "Robert N. Davoren Complex (RNDC)",
            "Rose M. Singer Center (RMSC)",
            "West Facility (WF)",
            "Bellevue Hospital Prison Ward (BHPW)",
            "Brooklyn Detention Complex (BKDC)",
            "Elmhurst Hospital Prison Ward (EHPW)",
            "Manhattan Detention Complex (MDC)",
            "Queens Detention Complex (QDC)",
            "Vernon C. Bain Center (VCBC)",
            "Correction Academy",
            "Bulova Building- DOC Headquarters",
        ]
        for i in soup.find_all("p"):
            info = i.get_text("|").split("|")
            if info[0] in targets:
                result = dict(
                    name=info[0],
                    address1=info[1],
                    address2=info[2],
                    house_number=self.get_hnum(info[1]),
                    street_name=self.get_sname(info[1]),
                    zipcode=self.get_zipcode(info[2]),
                )
                data.append(result)

        df = pd.DataFrame.from_records(data)
        return df

    def runner(self) -> str:
        df = self.ingest()
        local_path = df_to_tempfile(df)
        return local_path
