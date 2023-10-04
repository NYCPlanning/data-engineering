import os

from diagrams import Diagram, Cluster
from diagrams.custom import Custom
from diagrams.onprem import client
from diagrams.programming import flowchart
from diagrams.programming import language
from diagrams.generic.blank import Blank
from diagrams.generic.database import SQL
from diagrams.digitalocean import storage
from diagrams.aws.general import InternetAlt1

from library import config

Socrata = lambda name: Custom(name, "./data_flow_diagrams/resources/socrata.jpeg")

all_sources = [filename[:-4] for filename in os.listdir("./library/templates")]

cbbr = [
    "cbbr_submissions",
    "dpr_parksproperties",
    "dcp_facilities",
    "doitt_buildingfootprints",
]

ztl = [
    "dcp_commercialoverlay",
    "dcp_limitedheight",
    "dcp_specialpurpose",
    "dcp_specialpurposesubdistricts",
    "dcp_zoningmapamendments",
    "dof_dtm",
    "dcp_zoningdistricts",
    "dcp_zoningmapindex",
]

pluto = [
    "dcp_commercialoverlay",
    "dcp_limitedheight",
    "dcp_specialpurpose",
    "dcp_specialpurposesubdistricts",
    "dcp_zoningmapamendments",
    "dof_dtm",
    "dcp_zoningdistricts",
    "dcp_zoningmapindex",
    "dcp_cdboundaries_wi",
    "dcp_cb2010_wi",
    "dcp_ct2010_wi",
    "dcp_cb2020_wi",
    "dcp_ct2020_wi",
    "dcp_school_districts",
    "dcp_firecompanies",
    "dcp_policeprecincts",
    "dcp_councildistricts_wi",
    "dcp_healthareas",
    "dcp_healthcenters",
    "dof_shoreline",
    "doitt_zipcodeboundaries",
    "fema_firms2007_100yr",
    "fema_pfirms2015_100yr",
    "dpr_greenthumb",
    "dsny_frequencies",
    "lpc_historic_districts",
    "lpc_landmarks",
    "dcp_colp",
    "dof_condo",
]

source_list = {"cbbr": cbbr, "ztl": ztl, "pluto": pluto}

overrides: dict = {}

path = lambda name: f"./library/templates/{name}.yml"


class source:
    def __init__(self, name):
        if name in overrides:
            self.overrides = overrides[name]
        self.name = name
        config_obj = config.Config(path(name))
        raw_template = config_obj.parsed_rendered_template()
        # computed_template = config_obj.compute
        self.config = raw_template
        self.source_type = raw_template["dataset"]["source"]

    def source(self):
        if "url" in self.source_type or "path" in self.source_type:
            if "url" in self.source_type:
                path = self.source_type["url"]["path"]
            else:
                path = self.source_type["path"]
            if path.startswith("http"):
                split_path = path.split("//")[1].split("/")
                self.last_node = InternetAlt1(split_path[0] + "\n" + split_path[-1])
            elif path.startswith("library"):
                self.last_node = client.User(path[12:])
            elif path.startswith("s3"):
                s3path = path[5:]
                split_path = s3path.split("/")
                self.last_node = storage.Volume(split_path[0] + "\n" + split_path[2])
            else:
                self.last_node = client.User(path)
        elif "local_path" in self.source_type:
            self.last_node = client.User()
        elif "script" in self.source_type:
            self.last_node = language.Python(self.name + ".py")
        elif "socrata" in self.source_type:
            self.last_node = Socrata(self.name)  ## todo needs some more logic
        else:
            raise Exception(f"No found source for {self.name}")

    def processing(self):
        if "script" in self.source_type:
            node = language.Python("script ingestion")
            self.last_node >> node
            self.last_node = node
        node = language.Bash("standard processing")
        self.last_node >> node
        self.last_node = node

    def do(self):
        node = SQL(self.name)
        self.last_node >> node
        self.last_node = node
        self.do_node = node


def create_specific_diagram(name, sources):
    sources = [source(s) for s in sources]
    with Diagram(name, filename=f"./data_flow_diagrams/outputs/{name}"):
        for s in sources:
            s.source()

        with Cluster("Library Archive"):
            for s in sources:
                s.processing()

        with Cluster("Digital Ocean"):
            with Cluster("edm-recipes"):
                # do = storage.Volume('edm-recipes')
                for s in sources:
                    s.do()

        data_product = SQL(name)
        for s in sources:
            s.do_node >> data_product


def create_grand_diagram(names):
    sources = set([i for name in names for i in source_list[name]])
    sources = [source(s) for s in sources]
    with Diagram(
        "", direction="LR", filename="./data_flow_diagrams/outputs/full_diagram"
    ):
        for s in sources:
            s.source()

        with Cluster("Library Archive"):
            for s in sources:
                s.processing()

        with Cluster("Digital Ocean"):
            with Cluster("edm-recipes"):
                # do = storage.Volume('edm-recipes')
                for s in sources:
                    s.do()

        for name in names:
            data_product = SQL(name)
            for s in sources:
                if s.name in source_list[name]:
                    s.do_node >> data_product


create_specific_diagram("cbbr", cbbr)
create_specific_diagram("pluto", pluto)
create_specific_diagram("ztl", ztl)

# create_grand_diagram(["cbbr", "ztl"])
