import datetime
import re
from io import StringIO

import pandas as pd

from .geocode.function1B import Function1B
from .geocode.functionBL import FunctionBL
from .geocode.functionBN import FunctionBN
from .geocode.parseAddress import ParseAddress, UseAirportName
from .utility.export import Export
from .utility.prepare import Prepare


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="borough",
    zipcode_field="zipcode",
)
@ParseAddress(raw_address_field="address")
@Prepare
def bpl_libraries(df: pd.DataFrame = None):
    df["longitude"] = df.position.apply(lambda x: x.split(",")[1].strip())
    df["latitude"] = df.position.apply(lambda x: x.split(",")[0].strip())
    df["zipcode"] = df.address.apply(lambda x: x[-6:])
    df["borough"] = "Brooklyn"
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="locality",
    zipcode_field="zipcode",
)
@ParseAddress(raw_address_field="address")
@Prepare
def nypl_libraries(df: pd.DataFrame = None):
    df["latitude"] = df.lat
    df["longitude"] = df.lon
    return df


@Export
@Function1B(
    street_name_field="address_street_name",
    house_number_field="address_building",
    borough_field="address_borough",
    zipcode_field="address_zip",
)
@FunctionBL(bbl_field="bbl")
@FunctionBN(bin_field="bin")
@Prepare
def dca_operatingbusinesses(df: pd.DataFrame = None):
    industry = [
        "Scrap Metal Processor",
        "Parking Lot",
        "Garage",
        "Garage and Parking Lot",
        "Tow Truck Company",
    ]
    today = datetime.datetime.today()
    covid_freeze = datetime.datetime.strptime("03/12/2020", "%m/%d/%Y")
    df.license_expiration_date = pd.to_datetime(
        df["license_expiration_date"], format="%m/%d/%Y"
    )
    # fmt:off
    df = df.loc[((df.license_expiration_date >= today) & (df.industry == "Scrap Metal Processor"))|((df.license_expiration_date >= covid_freeze) & (df.industry != "Scrap Metal Processor")), :]\
        .loc[df.industry.isin(industry), :]
    # fmt:on
    return df


@Export
@FunctionBL(bbl_field="bbl")
@FunctionBN(bin_field="bin")
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="borough",
    zipcode_field="zipcode",
)
@ParseAddress(raw_address_field="address")
@Prepare
def dcla_culturalinstitutions(df: pd.DataFrame = None):
    df["zipcode"] = df.postcode.apply(lambda x: x[:5])
    return df


@Export
@FunctionBL(bbl_field="bbl")
@Function1B(
    street_name_field="sname", house_number_field="hnum", borough_field="borough"
)
@Prepare
def dcp_colp(df: pd.DataFrame = None):
    df["bbl"] = df.bbl.str.split(pat=".").str[0]
    return df


@Export
@Function1B(
    street_name_field="street_name",
    house_number_field="address_number",
    borough_field="borough_name",
    zipcode_field="zip_code",
)
@FunctionBL(bbl_field="bbl")
@FunctionBN(bin_field="bin")
@Prepare
def dcp_pops(df: pd.DataFrame = None):
    return df


@Export
@Prepare
def dcp_sfpsd(df: pd.DataFrame = None):
    return df


@Export
@Function1B(
    street_name_field="street_name",
    house_number_field="house_number",
    zipcode_field="zipcode",
)
@Prepare
def dep_wwtc(df: pd.DataFrame = None):
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    zipcode_field="program_zipcode",
)
@ParseAddress(raw_address_field="program_address")
@Prepare
def dfta_contracts(df: pd.DataFrame = None):
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="garage_city",
    zipcode_field="garage_zip",
)
@ParseAddress(raw_address_field="garage__street_address")
@Prepare
def doe_busroutesgarages(df: pd.DataFrame = None):
    # SCHOOL_YEAR = f"{datetime.date.today().year-1}-{datetime.date.today().year}"
    SCHOOL_YEAR = "2019-2020"  # No records from 2020-2021 school year
    df = df.loc[df.school_year == SCHOOL_YEAR, :]
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="city",
    zipcode_field="zip",
)
@FunctionBL(bbl_field="borough_block_lot")
@ParseAddress(raw_address_field="primary_address")
@Prepare
def doe_lcgms(df: pd.DataFrame = None):
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="boro",
    zipcode_field="zip",
)
@ParseAddress(raw_address_field="address")
@Prepare
def doe_universalprek(df: pd.DataFrame = None):
    df["boro"] = df.borough.map(
        {
            "M": "Manhattan",
            "X": "Bronx",
            "B": "Brooklyn",
            "Q": "Queens",
            "R": "Staten Island",
        }
    )
    return df


@Export
@Function1B(
    street_name_field="street",
    house_number_field="building",
    borough_field="borough",
    zipcode_field="zipcode",
)
@FunctionBN(bin_field="building_identification_number")
@Prepare
def dohmh_daycare(df: pd.DataFrame = None):
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="boroname",
)
@ParseAddress(raw_address_field="site")
@Prepare
def dot_bridgehouses(df: pd.DataFrame = None):
    df["address"] = df.address.astype(str).replace("n/a", None)
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="boroname",
)
@FunctionBL(bbl_field="bbl")
@ParseAddress(raw_address_field="address")
@Prepare
def dot_ferryterminals(df: pd.DataFrame = None):
    df["bbl"] = df.bbl.fillna(0).astype(float).astype(int)
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="boroname",
)
@FunctionBL(bbl_field="bbl")
@ParseAddress(raw_address_field="address")
@Prepare
def dot_mannedfacilities(df: pd.DataFrame = None):
    df["address"] = df.address.astype(str)
    df["bbl"] = df.bbl.fillna(0).astype(float).astype(int)
    return df


@Export
@Prepare
def dot_pedplazas(df: pd.DataFrame = None):
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="boroname",
)
@FunctionBL(bbl_field="bbl")
@ParseAddress(raw_address_field="address")
@Prepare
def dot_publicparking(df: pd.DataFrame = None):
    df["address"] = df.address.astype(str)
    df["bbl"] = df.bbl.fillna(0).astype(float).astype(int)
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="boro",
    zipcode_field="zipcode",
)
@ParseAddress(raw_address_field="address")
@Prepare
def dpr_parksproperties(df: pd.DataFrame = None):
    df["zipcode"] = df.zipcode.astype(str).apply(lambda x: x[:5])
    df["boro"] = df.borough.map(
        {
            "M": "Manhattan",
            "X": "Bronx",
            "B": "Brooklyn",
            "Q": "Queens",
            "R": "Staten Island",
        }
    )
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="boro",
    zipcode_field="zip",
)
@ParseAddress(raw_address_field="address")
@FunctionBL(bbl_field="bbl")
@FunctionBN(bin_field="bin")
@Prepare
def dsny_garages(df: pd.DataFrame = None):
    df["address"] = df.address.astype(str)
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="boro",
    zipcode_field="zip",
)
@ParseAddress(raw_address_field="address")
@Prepare
def dsny_specialwastedrop(df: pd.DataFrame = None):
    return df


@Export
@Function1B(
    street_name_field="street", house_number_field="number", borough_field="borough"
)
@FunctionBL(bbl_field="bbl")
@FunctionBN(bin_field="bin")
@Prepare
def dsny_donatenycdirectory(df: pd.DataFrame = None):
    df["bbl"] = df.bbl.fillna(0).astype(float).astype(int)
    df = df[
        df["categoriesaccepted"].notna()
        & df["categoriesaccepted"].str.contains("Clothing")
    ]
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="borough",
    zipcode_field="zipcode",
)
@ParseAddress(raw_address_field="address")
@FunctionBL(bbl_field="bbl")
@FunctionBN(bin_field="bin")
@Prepare
def dsny_leafdrop(df: pd.DataFrame = None):
    df["bbl"] = df.bbl.fillna(0).astype(float).astype(int)
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="borough",
    zipcode_field="zip_code",
)
@ParseAddress(raw_address_field="location")
@Prepare
def dsny_fooddrop(df: pd.DataFrame = None):
    def _loc_to_zipcode(location):
        location_str = str(location)
        return location_str[:-5] if location_str[:-5].isnumeric() else None

    df["zip_code"] = df["location"].apply(_loc_to_zipcode)
    return df


@Export
@Function1B(
    street_name_field="street",
    house_number_field="number",
    borough_field="borough",
    zipcode_field="zipcode",
)
@FunctionBL(bbl_field="bbl")
@FunctionBN(bin_field="bin")
@Prepare
def dsny_electronicsdrop(df: pd.DataFrame = None):
    df["bbl"] = df.bbl.fillna(0).astype(float).astype(int)
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="borough__community",
    zipcode_field="zipcode",
)
@FunctionBL(bbl_field="bbl")
@FunctionBN(bin_field="bin")
@ParseAddress(raw_address_field="address")
@Prepare
def dycd_afterschoolprograms(df: pd.DataFrame = None):
    df["address"] = df.location_1.apply(
        lambda x: str(x).replace("nan", "").split("\n")[0][:-5]
    )
    df["zipcode"] = df.location_1.apply(
        lambda x: str(x).replace("nan", "").split("\n")[0][-5:]
    )
    df["spatial"] = df.location_1.apply(
        lambda x: str(x).replace("nan", "").split("\n")[-1]
    )
    df["spatial"] = df.spatial.apply(lambda x: x.replace("(", "").replace(")", ""))
    df["longitude"] = df.spatial.apply(lambda x: x.split(",")[-1])
    df["latitude"] = df.spatial.apply(lambda x: x.split(",")[0])
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="city",
    zipcode_field="zipcode",
)
@ParseAddress(raw_address_field="address")
@Prepare
def fbop_corrections(df: pd.DataFrame = None):
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="borough",
    zipcode_field="postcode",
)
@FunctionBL(bbl_field="bbl")
@FunctionBN(bin_field="bin")
@ParseAddress(raw_address_field="cleaned_address")
@Prepare
def fdny_firehouses(df: pd.DataFrame = None):
    df["cleaned_address"] = df["facilityaddress"].map(
        lambda x: re.sub("[^A-Za-z0-9- ]+", "", x)
    )
    research = (
        "facilityname,facilityaddress,borough,postcode,wkt\n"
        'Engine 261/Ladder 116,37-20 29 street,Queens,11101,"POINT (-73.9354517 40.755397)"\n'
        'Marine 1,Little West 12th Street/Hudson River,Manhattan,10014,"POINT (-74.0118215 40.7406884)"\n'
        'Engine 307/Ladder 154,81-17 Northern Blvd,Queens,11372,"POINT (-73.8877877 40.755805)"\n'
    )
    df_research = pd.read_csv(StringIO(research))
    df.loc[
        df.facilityname.isin(df_research.facilityname),
        ["facilityaddress", "borough", "postcode", "wkt"],
    ] = df_research.loc[
        df_research.facilityname.isin(df.facilityname),
        ["facilityaddress", "borough", "postcode", "wkt"],
    ].values
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    zipcode_field="zip_code",
)
@ParseAddress(raw_address_field="address")
@Prepare
def foodbankny_foodbanks(df: pd.DataFrame = None):
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="borough",
    zipcode_field="postcode",
)
@ParseAddress(raw_address_field="location_1")
@FunctionBL(bbl_field="bbl")
@FunctionBN(bin_field="bin")
@Prepare
def hhc_hospitals(df: pd.DataFrame = None):
    df["spatial"] = df.location_1.apply(lambda x: x.split("(")[-1].replace(")", ""))
    df["longitude"] = df.spatial.apply(lambda x: x.split(",")[-1])
    df["latitude"] = df.spatial.apply(lambda x: x.split(",")[0])
    df.location_1 = df.location_1.apply(lambda x: x.replace("\n", " "))
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="borough",
    zipcode_field="postcode",
)
@ParseAddress(raw_address_field="street_address")
@FunctionBL(bbl_field="bbl")
@FunctionBN(bin_field="bin")
@Prepare
def hra_snapcenters(df: pd.DataFrame = None):
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="borough",
    zipcode_field="postcode",
)
@ParseAddress(raw_address_field="street_address")
@FunctionBL(bbl_field="bbl")
@FunctionBN(bin_field="bin")
@Prepare
def hra_jobcenters(df: pd.DataFrame = None):
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="borough",
    zipcode_field="postcode",
)
@ParseAddress(raw_address_field="office_address")
@FunctionBL(bbl_field="bbl")
@FunctionBN(bin_field="bin")
@Prepare
def hra_medicaid(df: pd.DataFrame = None):
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="borough",
    zipcode_field="postcode",
)
@ParseAddress(raw_address_field="address_1")
@FunctionBL(bbl_field="bbl")
@FunctionBN(bin_field="bin")
@Prepare
def moeo_socialservicesitelocations(df: pd.DataFrame = None):
    df["borough"] = df.borough.str.upper()
    df["bbl"] = df.bbl.replace("undefinedundefinedundefined", None)
    df["bbl"] = df.bbl.fillna(0).astype(float).astype(int)
    df["bin"] = df.bin.fillna(0).astype(float).astype(int)
    df["postcode"] = df.bbl.fillna(0).astype(float).astype(int)
    df["postcode"] = df.postcode.astype(str).apply(lambda x: x[:5])
    return df


@Export
@Function1B(
    street_name_field="street_name",
    house_number_field="house_number",
    zipcode_field="zipcode",
)
@Prepare
def nycdoc_corrections(df: pd.DataFrame = None):
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="borough",
)
@ParseAddress(raw_address_field="address")
@FunctionBL(bbl_field="bbl")
@FunctionBN(bin_field="bin")
@Prepare
def nycha_communitycenters(df: pd.DataFrame = None):
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="borough",
    zipcode_field="zipcode",
)
@ParseAddress(raw_address_field="address")
@Prepare
def nycha_policeservice(df: pd.DataFrame = None):
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="borough",
    zipcode_field="zipcode",
)
@ParseAddress(raw_address_field="address")
@Prepare
def nycourts_courts(df: pd.DataFrame = None):
    return df


@Export
@Prepare
def nysdec_lands(df: pd.DataFrame = None):
    df = df[df.county.isin(["NEW YORK", "KINGS", "BRONX", "QUEENS", "RICHMOND"])]
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="county",
    zipcode_field="zip_code",
)
@ParseAddress(raw_address_field="location_address")
@Prepare
def nysdec_solidwaste(df: pd.DataFrame = None):
    df = df[df.county.isin(["New York", "Kings", "Bronx", "Queens", "Richmond"])]
    return df


@Export
@Function1B(
    street_name_field="street_name",
    house_number_field="house_number",
    borough_field="county",
    zipcode_field="zipcode",
)
@Prepare
def nysdoccs_corrections(df: pd.DataFrame = None):
    df["zipcode"] = df.zipcode.apply(lambda x: x[:5])
    df = df[df.county.isin(["New York", "Kings", "Bronx", "Queens", "Richmond"])]
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="facility_county",
    zipcode_field="zipcode",
)
@ParseAddress(raw_address_field="facility_address_1")
@Prepare
def nysdoh_healthfacilities(df: pd.DataFrame = None):
    df = df.loc[
        df.facility_county.isin(["New York", "Kings", "Bronx", "Queens", "Richmond"]), :
    ].copy()
    df["zipcode"] = df.facility_zip_code.apply(lambda x: x[:5])
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="county",
    zipcode_field="zip",
)
@ParseAddress(raw_address_field="street_address")
@Prepare
def nysdoh_nursinghomes(df: pd.DataFrame = None):
    df = df[df.county.isin(["New York", "Kings", "Bronx", "Queens", "Richmond"])]
    return df


@Export
@Prepare
def nysed_nonpublicenrollment(df: pd.DataFrame = None):
    return df


@Export
@Prepare
def sca_enrollment_capacity(df: pd.DataFrame = None):
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="county_description",
    zipcode_field="physical_zipcd5",
)
@ParseAddress(raw_address_field="physical_address_line1")
@Prepare
def nysed_activeinstitutions(df: pd.DataFrame = None):
    df = df[
        df.county_description.isin(["NEW YORK", "KINGS", "BRONX", "QUEENS", "RICHMOND"])
    ]
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="provider_county",
    zipcode_field="provider_zip_code",
)
@ParseAddress(raw_address_field="provider_street")
@Prepare
def nysoasas_programs(df: pd.DataFrame = None):
    df = df[
        df.provider_county.isin(["New York", "Kings", "Bronx", "Queens", "Richmond"])
    ]
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="program_county",
    zipcode_field="program_zip",
)
@ParseAddress(raw_address_field="program_address_1")
@Prepare
def nysomh_mentalhealth(df: pd.DataFrame = None):
    df = df[
        df.program_county.isin(["New York", "Kings", "Bronx", "Queens", "Richmond"])
    ]
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="county",
    zipcode_field="zip_code",
)
@ParseAddress(raw_address_field="street_address")
@Prepare
def nysopwdd_providers(df: pd.DataFrame = None):
    df = df[df.county.isin(["BRONX", "NEW YORK", "KINGS", "QUEENS", "RICHMOND"])]
    return df


@Export
@Prepare
def nysparks_historicplaces(df: pd.DataFrame = None):
    df = df[df.county.isin(["Bronx", "New York", "Kings", "Queens", "Richmond"])]
    return df


@Export
@Prepare
def nysparks_parks(df: pd.DataFrame = None):
    df = df[df.county.isin(["Bronx", "New York", "Kings", "Queens", "Richmond"])]
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="borough",
    zipcode_field="postcode",
)
@FunctionBN(bin_field="bin")
@FunctionBL(bbl_field="bbl")
@ParseAddress(raw_address_field="address")
@Prepare
def qpl_libraries(df: pd.DataFrame = None):
    return df


@Export
@Function1B(
    street_name_field="street",
    house_number_field="number",
    borough_field="borough",
    zipcode_field="postcode",
)
@FunctionBN(bin_field="bin")
@FunctionBL(bbl_field="bbl")
@Prepare
def sbs_workforce1(df: pd.DataFrame = None):
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    zipcode_field="zipcode",
)
@ParseAddress(raw_address_field="buildingaddress")
@Prepare
def uscourts_courts(df: pd.DataFrame = None):
    df["zipcode"] = df.buildingzip.apply(lambda x: x[:5])
    return df


@Export
@Function1B(
    street_name_field="parsed_sname",
    house_number_field="parsed_hnum",
    borough_field="county",
    zipcode_field="zipcode",
)
@UseAirportName
@ParseAddress(raw_address_field="manager_address")
@Prepare
def usdot_airports(df: pd.DataFrame = None):
    df = df.loc[
        (df["state_name"] == "NEW YORK")
        & (df.county.isin(["NEW YORK", "KINGS", "BRONX", "QUEENS", "RICHMOND"])),
        :,
    ].copy()
    # 1B can geocode free form address if we pass it into street_name
    df["zipcode"] = df["manager_city_state_zip"].str[-5:]
    df["airport_name"] = ""
    df.loc[
        df.name == "JOHN F KENNEDY INTL", "airport_name"
    ] = "JOHN F KENNEDY INTL AIRPORT"
    df.loc[df.name == "LAGUARDIA", "airport_name"] = "LAGUARDIA AIRPORT"
    return df


@Export
@Prepare
def usdot_ports(df: pd.DataFrame = None):
    df = df.loc[
        (df.state_post == "NY")
        & (df.county_nam.isin(["New York", "Kings", "Bronx", "Queens", "Richmond"])),
        :,
    ]
    return df


@Export
@Prepare
def usnps_parks(df: pd.DataFrame = None):
    df = df[df.state == "NY"]
    return df
