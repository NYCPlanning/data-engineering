import datetime
import importlib
import re
from io import StringIO

import pandas as pd

from .geocode.function1B import Function1B
from .geocode.functionBL import FunctionBL
from .geocode.functionBN import FunctionBN
from .geocode.parseAddress import parse_address, use_airport_name
from facdb.utility.utils import sanitize_df, format_field_names, hash_each_row


def bpl_libraries(df: pd.DataFrame):
    df["zipcode"] = df.address.apply(lambda x: x[-6:])
    df["borough"] = "Brooklyn"
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="address")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="borough",
        zipcode_field="zipcode",
    ).geocode_a_dataframe(df)
    return df


def nypl_libraries(df: pd.DataFrame):
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="address")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="locality",
        zipcode_field="zipcode",
    ).geocode_a_dataframe(df)
    return df


def dca_operatingbusinesses(df: pd.DataFrame):
    industry = [
        "Scrap Metal Processor",
        "Parking Lot",
        "Garage",
        "Garage and Parking Lot",
        "Garage & Parking Lot",
        "Tow Truck Company",
    ]
    today = datetime.datetime.today()
    covid_freeze = datetime.datetime.strptime("03/12/2020", "%m/%d/%Y")
    df.expiration_date = pd.to_datetime(df["expiration_date"], format="%m/%d/%Y")
    # fmt:off
    df = df.loc[((df.expiration_date >= today) & (df.business_category == "Scrap Metal Processor"))|((df.expiration_date >= covid_freeze) & (df.business_category != "Scrap Metal Processor")), :]\
        .loc[df.business_category.isin(industry), :]
    # fmt:on
    df = sanitize_df(df)
    df = FunctionBN(bin_field="bin").geocode_a_dataframe(df)
    df = FunctionBL(bbl_field="bbl").geocode_a_dataframe(df)
    df = Function1B(
        street_name_field="address_street_name",
        house_number_field="address_building",
        borough_field="address_borough",
        zipcode_field="address_zip",
    ).geocode_a_dataframe(df)
    return df


def dcla_culturalinstitutions(df: pd.DataFrame):
    df["zipcode"] = df.postcode.apply(lambda x: x[:5])
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="address")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="borough",
        zipcode_field="zipcode",
    ).geocode_a_dataframe(df)
    df = FunctionBN(bin_field="bin").geocode_a_dataframe(df)
    df = FunctionBL(bbl_field="bbl").geocode_a_dataframe(df)
    return df


def dcp_colp(df: pd.DataFrame):
    df["bbl"] = df.bbl.str.split(pat=".").str[0]
    df = sanitize_df(df)
    df = Function1B(
        street_name_field="sname", house_number_field="hnum", borough_field="borough"
    ).geocode_a_dataframe(df)
    df = FunctionBL(bbl_field="bbl").geocode_a_dataframe(df)
    return df


def dcp_pops(df: pd.DataFrame):
    df = sanitize_df(df)
    df = FunctionBN(bin_field="bin").geocode_a_dataframe(df)
    df = FunctionBL(bbl_field="bbl").geocode_a_dataframe(df)
    df = Function1B(
        street_name_field="street_name",
        house_number_field="address_number",
        borough_field="borough_name",
        zipcode_field="zip_code",
    ).geocode_a_dataframe(df)
    return df


def dcp_sfpsd(df: pd.DataFrame):
    df = sanitize_df(df)
    return df


def dep_wwtc(df: pd.DataFrame):
    df = sanitize_df(df)
    df = Function1B(
        street_name_field="street_name",
        house_number_field="house_number",
        zipcode_field="zipcode",
    ).geocode_a_dataframe(df)
    return df


def dfta_contracts(df: pd.DataFrame):
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="programaddress")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        zipcode_field="postcode",
    ).geocode_a_dataframe(df)
    return df


def doe_busroutesgarages(df: pd.DataFrame):
    # SCHOOL_YEAR = f"{datetime.date.today().year-1}-{datetime.date.today().year}"
    SCHOOL_YEAR = "2019-2020"  # No records from 2020-2021 school year
    df = df.loc[df.school_year == SCHOOL_YEAR, :]
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="garage__street_address")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="garage_city",
        zipcode_field="garage_zip",
    ).geocode_a_dataframe(df)
    return df


def doe_lcgms(df: pd.DataFrame):
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="primary_address")
    df = FunctionBL(bbl_field="borough_block_lot").geocode_a_dataframe(df)
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="city",
        zipcode_field="zip",
    ).geocode_a_dataframe(df)
    return df


def doe_universalprek(df: pd.DataFrame):
    df["boro"] = df.borough.map(
        {
            "M": "Manhattan",
            "X": "Bronx",
            "B": "Brooklyn",
            "Q": "Queens",
            "R": "Staten Island",
        }
    )
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="siteaddress")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="boro",
        zipcode_field="state",
    ).geocode_a_dataframe(df)
    return df


def dohmh_daycare(df: pd.DataFrame):
    df = sanitize_df(df)
    df = FunctionBN(bin_field="building_identification_number").geocode_a_dataframe(df)
    df = Function1B(
        street_name_field="street",
        house_number_field="building",
        borough_field="borough",
        zipcode_field="zipcode",
    ).geocode_a_dataframe(df)
    return df


def dot_bridgehouses(df: pd.DataFrame):
    df["address"] = df.address.astype(str).replace("n/a", None)
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="site")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="boroname",
    ).geocode_a_dataframe(df)
    return df


def dot_ferryterminals(df: pd.DataFrame):
    df["bbl"] = df.bbl.fillna(0).astype(float).astype(int)
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="address")
    df = FunctionBL(bbl_field="bbl").geocode_a_dataframe(df)
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="boroname",
    ).geocode_a_dataframe(df)
    return df


def dot_mannedfacilities(df: pd.DataFrame):
    df["address"] = df.address.astype(str)
    df["bbl"] = df.bbl.fillna(0).astype(float).astype(int)
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="address")
    df = FunctionBL(bbl_field="bbl").geocode_a_dataframe(df)
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="boroname",
    ).geocode_a_dataframe(df)
    return df


def dot_pedplazas(df: pd.DataFrame):
    df = sanitize_df(df)
    return df


def dot_publicparking(df: pd.DataFrame):
    df["address"] = df.address.astype(str)
    df["bbl"] = df.bbl.fillna(0).astype(float).astype(int)
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="address")
    df = FunctionBL(bbl_field="bbl").geocode_a_dataframe(df)
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="boroname",
    ).geocode_a_dataframe(df)
    return df


def dpr_parksproperties(df: pd.DataFrame):
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
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="address")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="boro",
        zipcode_field="zipcode",
    ).geocode_a_dataframe(df)
    return df


def dsny_garages(df: pd.DataFrame):
    df["address"] = df.address.astype(str)
    df = sanitize_df(df)
    df = FunctionBN(bin_field="bin").geocode_a_dataframe(df)
    df = FunctionBL(bbl_field="bbl").geocode_a_dataframe(df)
    df = parse_address(df, raw_address_field="address")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="boro",
        zipcode_field="zip",
    ).geocode_a_dataframe(df)
    return df


def dsny_specialwastedrop(df: pd.DataFrame):
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="address")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="boro",
        zipcode_field="zip",
    ).geocode_a_dataframe(df)
    return df


def dsny_donatenycdirectory(df: pd.DataFrame):
    df["bbl"] = df.bbl.fillna(0).astype(float).astype(int)
    df = df[
        df["categoriesaccepted"].notna()
        & df["categoriesaccepted"].str.contains("Clothing")
    ]
    df = sanitize_df(df)
    df = FunctionBN(bin_field="bin").geocode_a_dataframe(df)
    df = FunctionBL(bbl_field="bbl").geocode_a_dataframe(df)
    df = Function1B(
        street_name_field="street", house_number_field="number", borough_field="borough"
    ).geocode_a_dataframe(df)
    return df


def dsny_leafdrop(df: pd.DataFrame):
    df["bbl"] = df.bbl.fillna(0).astype(float).astype(int)
    df = sanitize_df(df)
    df = FunctionBN(bin_field="bin").geocode_a_dataframe(df)
    df = FunctionBL(bbl_field="bbl").geocode_a_dataframe(df)
    df = parse_address(df, raw_address_field="address")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="borough",
        zipcode_field="zipcode",
    ).geocode_a_dataframe(df)
    return df


def dsny_fooddrop(df: pd.DataFrame):
    def _loc_to_zipcode(location):
        location_str = str(location)
        return location_str[:-5] if location_str[:-5].isnumeric() else None

    df["zip_code"] = df["location"].apply(_loc_to_zipcode)
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="location")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="borough",
        zipcode_field="zip_code",
    ).geocode_a_dataframe(df)
    return df


def dsny_electronicsdrop(df: pd.DataFrame):
    df["bbl"] = df.bbl.fillna(0).astype(float).astype(int)
    df = sanitize_df(df)
    df = FunctionBN(bin_field="bin").geocode_a_dataframe(df)
    df = FunctionBL(bbl_field="bbl").geocode_a_dataframe(df)
    df = Function1B(
        street_name_field="street",
        house_number_field="number",
        borough_field="borough",
        zipcode_field="zipcode",
    ).geocode_a_dataframe(df)
    return df


def dycd_service_sites(df: pd.DataFrame):
    def _nan_to_string(value: object) -> str:
        return str(value).replace("nan", "")

    df["address"] = df["street_address"].apply(_nan_to_string)
    df["zipcode"] = df["postcode"].apply(_nan_to_string)
    df["latitude"] = df["latitude"].apply(_nan_to_string)
    df["longitude"] = df["longitude"].apply(_nan_to_string)
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="address")
    df = FunctionBN(bin_field="bin").geocode_a_dataframe(df)
    df = FunctionBL(bbl_field="bbl").geocode_a_dataframe(df)
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="borough__community",
        zipcode_field="zipcode",
    ).geocode_a_dataframe(df)
    return df


def fbop_corrections(df: pd.DataFrame):
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="address")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="city",
        zipcode_field="zipcode",
    ).geocode_a_dataframe(df)
    return df


def fdny_firehouses(df: pd.DataFrame):
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
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="cleaned_address")
    df = FunctionBN(bin_field="bin").geocode_a_dataframe(df)
    df = FunctionBL(bbl_field="bbl").geocode_a_dataframe(df)
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="borough",
        zipcode_field="postcode",
    ).geocode_a_dataframe(df)
    return df


def foodbankny_foodbanks(df: pd.DataFrame):
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="address")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        zipcode_field="zip_code",
    ).geocode_a_dataframe(df)
    return df


def hhc_hospitals(df: pd.DataFrame):
    df["spatial"] = df.location_1.apply(lambda x: x.split("(")[-1].replace(")", ""))
    df["longitude"] = df.spatial.apply(lambda x: x.split(",")[-1])
    df["latitude"] = df.spatial.apply(lambda x: x.split(",")[0])
    df.location_1 = df.location_1.apply(lambda x: x.replace("\n", " "))
    df = sanitize_df(df)
    df = FunctionBN(bin_field="bin").geocode_a_dataframe(df)
    df = FunctionBL(bbl_field="bbl").geocode_a_dataframe(df)
    df = parse_address(df, raw_address_field="location_1")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="borough",
        zipcode_field="postcode",
    ).geocode_a_dataframe(df)
    return df


def hra_snapcenters(df: pd.DataFrame):
    df = sanitize_df(df)
    df = FunctionBN(bin_field="bin").geocode_a_dataframe(df)
    df = FunctionBL(bbl_field="bbl").geocode_a_dataframe(df)
    df = parse_address(df, raw_address_field="street_address")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="borough",
        zipcode_field="postcode",
    ).geocode_a_dataframe(df)
    return df


def hra_jobcenters(df: pd.DataFrame):
    df = sanitize_df(df)
    df = FunctionBN(bin_field="bin").geocode_a_dataframe(df)
    df = FunctionBL(bbl_field="bbl").geocode_a_dataframe(df)
    df = parse_address(df, raw_address_field="street_address")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="borough",
        zipcode_field="postcode",
    ).geocode_a_dataframe(df)
    return df


def hra_medicaid(df: pd.DataFrame):
    df = sanitize_df(df)
    df = FunctionBN(bin_field="bin").geocode_a_dataframe(df)
    df = FunctionBL(bbl_field="bbl").geocode_a_dataframe(df)
    df = parse_address(df, raw_address_field="street_address")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="borough",
        zipcode_field="postcode",
    ).geocode_a_dataframe(df)
    return df


def moeo_socialservicesitelocations(df: pd.DataFrame):
    df["borough"] = df.borough.str.upper()
    df["bbl"] = df.bbl.replace("undefinedundefinedundefined", None)
    df["bbl"] = df.bbl.fillna(0).astype(float).astype(int)
    df["bin"] = df.bin.fillna(0).astype(float).astype(int)
    df["postcode"] = df.bbl.fillna(0).astype(float).astype(int)
    df["postcode"] = df.postcode.astype(str).apply(lambda x: x[:5])
    df = sanitize_df(df)
    df = FunctionBN(bin_field="bin").geocode_a_dataframe(df)
    df = FunctionBL(bbl_field="bbl").geocode_a_dataframe(df)
    df = parse_address(df, raw_address_field="address_1")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="borough",
        zipcode_field="postcode",
    ).geocode_a_dataframe(df)
    return df


def nycdoc_corrections(df: pd.DataFrame):
    df = sanitize_df(df)
    df = Function1B(
        street_name_field="street_name",
        house_number_field="house_number",
        zipcode_field="zipcode",
    ).geocode_a_dataframe(df)
    return df


def nycha_communitycenters(df: pd.DataFrame):
    df = sanitize_df(df)
    df = FunctionBN(bin_field="bin").geocode_a_dataframe(df)
    df = FunctionBL(bbl_field="bbl").geocode_a_dataframe(df)
    df = parse_address(df, raw_address_field="address")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="borough",
    ).geocode_a_dataframe(df)
    return df


def nycha_policeservice(df: pd.DataFrame):
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="address")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="borough",
        zipcode_field="zipcode",
    ).geocode_a_dataframe(df)
    return df


def nycourts_courts(df: pd.DataFrame):
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="address")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="borough",
        zipcode_field="zipcode",
    ).geocode_a_dataframe(df)
    return df


def nysdec_lands(df: pd.DataFrame):
    df = df[df.county.isin(["NEW YORK", "KINGS", "BRONX", "QUEENS", "RICHMOND"])]
    df = sanitize_df(df)
    return df


def nysdec_solidwaste(df: pd.DataFrame):
    df = df[df.county.isin(["New York", "Kings", "Bronx", "Queens", "Richmond"])]
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="location_address")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="county",
        zipcode_field="zip_code",
    ).geocode_a_dataframe(df)
    return df


def nysdoccs_corrections(df: pd.DataFrame):
    df["zipcode"] = df.zipcode.apply(lambda x: x[:5])
    df = df[df.county.isin(["New York", "Kings", "Bronx", "Queens", "Richmond"])]
    df = sanitize_df(df)
    df = Function1B(
        street_name_field="street_name",
        house_number_field="house_number",
        borough_field="county",
        zipcode_field="zipcode",
    ).geocode_a_dataframe(df)
    return df


def nysdoh_healthfacilities(df: pd.DataFrame):
    df = df.loc[
        df.facility_county.isin(["New York", "Kings", "Bronx", "Queens", "Richmond"]), :
    ].copy()
    df["zipcode"] = df.facility_zip_code.apply(lambda x: x[:5])
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="facility_address_1")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="facility_county",
        zipcode_field="zipcode",
    ).geocode_a_dataframe(df)
    return df


def nysdoh_nursinghomes(df: pd.DataFrame):
    df = df[df.county.isin(["New York", "Kings", "Bronx", "Queens", "Richmond"])]
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="street_address")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="county",
        zipcode_field="zip",
    ).geocode_a_dataframe(df)
    return df


def nysed_nonpublicenrollment(df: pd.DataFrame):
    df = sanitize_df(df)
    return df


def sca_enrollment_capacity(df: pd.DataFrame):
    df = sanitize_df(df)
    return df


def nysed_activeinstitutions(df: pd.DataFrame):
    df = df[
        df.county_description.isin(["NEW YORK", "KINGS", "BRONX", "QUEENS", "RICHMOND"])
    ]
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="physical_address_line1")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="county_description",
        zipcode_field="physical_zipcd5",
    ).geocode_a_dataframe(df)
    return df


def nysoasas_programs(df: pd.DataFrame):
    df = df[
        df.provider_county.isin(["New York", "Kings", "Bronx", "Queens", "Richmond"])
    ]
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="provider_street")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="provider_county",
        zipcode_field="provider_zip_code",
    ).geocode_a_dataframe(df)
    return df


def nysomh_mentalhealth(df: pd.DataFrame):
    df = df[
        df.program_county.isin(["New York", "Kings", "Bronx", "Queens", "Richmond"])
    ]
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="program_address_1")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="program_county",
        zipcode_field="program_zip",
    ).geocode_a_dataframe(df)
    return df


def nysopwdd_providers(df: pd.DataFrame):
    df = df[df.county.isin(["BRONX", "NEW YORK", "KINGS", "QUEENS", "RICHMOND"])]
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="street_address")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="county",
        zipcode_field="zip_code",
    ).geocode_a_dataframe(df)
    return df


def nysparks_historicplaces(df: pd.DataFrame):
    df = df[df.countyname.isin(["Bronx", "New York", "Kings", "Queens", "Richmond"])]
    df = sanitize_df(df)
    return df


def nysparks_parks(df: pd.DataFrame):
    df = df[df.county.isin(["Bronx", "New York", "Kings", "Queens", "Richmond"])]
    df = sanitize_df(df)
    return df


def qpl_libraries(df: pd.DataFrame):
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="address")
    df = FunctionBL(bbl_field="bbl").geocode_a_dataframe(df)
    df = FunctionBN(bin_field="bin").geocode_a_dataframe(df)
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="borough",
        zipcode_field="postcode",
    ).geocode_a_dataframe(df)
    return df


def sbs_workforce1(df: pd.DataFrame):
    df = sanitize_df(df)
    df = FunctionBL(bbl_field="bbl").geocode_a_dataframe(df)
    df = FunctionBN(bin_field="bin").geocode_a_dataframe(df)
    df = Function1B(
        street_name_field="street",
        house_number_field="number",
        borough_field="borough",
        zipcode_field="postcode",
    ).geocode_a_dataframe(df)
    return df


def uscourts_courts(df: pd.DataFrame):
    df["zipcode"] = df.buildingzip.apply(lambda x: x[:5])
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="buildingaddress")
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        zipcode_field="zipcode",
    ).geocode_a_dataframe(df)
    return df


def usdot_airports(df: pd.DataFrame):
    df = df.loc[
        (df["state_name"] == "NEW YORK")
        & (df.county.isin(["NEW YORK", "KINGS", "BRONX", "QUEENS", "RICHMOND"])),
        :,
    ].copy()
    # 1B can geocode free form address if we pass it into street_name
    df["zipcode"] = df["manager_city_state_zip"].str[-5:]
    df["airport_name"] = ""
    df.loc[df.name == "JOHN F KENNEDY INTL", "airport_name"] = (
        "JOHN F KENNEDY INTL AIRPORT"
    )
    df.loc[df.name == "LAGUARDIA", "airport_name"] = "LAGUARDIA AIRPORT"
    df = sanitize_df(df)
    df = parse_address(df, raw_address_field="manager_address")
    df = use_airport_name(df)
    df = Function1B(
        street_name_field="parsed_sname",
        house_number_field="parsed_hnum",
        borough_field="county",
        zipcode_field="zipcode",
    ).geocode_a_dataframe(df)
    return df


def usdot_ports(df: pd.DataFrame):
    df = df.loc[
        (df.state_post == "NY")
        & (df.county_nam.isin(["New York", "Kings", "Bronx", "Queens", "Richmond"])),
        :,
    ]
    df = sanitize_df(df)
    return df


def usnps_parks(df: pd.DataFrame):
    df = df[df.state == "NY"]
    df = sanitize_df(df)
    return df


def dispatch(name: str, df: pd.DataFrame):
    """Dispatch a pipeline defined in this module."""
    pipelines = importlib.import_module(__name__)
    pipeline = getattr(pipelines, name)

    df["source"] = name  # legacy column. Easier to add here than modify SQL files
    return df.pipe(hash_each_row).pipe(format_field_names).pipe(pipeline)
