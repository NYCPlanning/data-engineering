from multiprocessing import Pool, cpu_count
from sqlalchemy import create_engine, text
from geosupport import Geosupport, GeosupportError
from utils import psql_insert_copy
import pandas as pd
from pydantic import BaseModel
import json
import os

g = Geosupport()


class BBL(BaseModel):
    boro: int
    block: int
    lot: int

    def __repr__(self):
        return f"{self.boro}{self.block:05}{self.lot:04}"


class GeocodeResult(BaseModel):
    boro_name: str
    house_number: str
    house_number_sort_format: str
    b10sc: str
    street_name: str
    b10sc_2: str | None
    street_name_2: str | None
    b10sc_3: str | None
    street_name_3: str | None
    bbl: BBL
    filler_for_tax_lot_version_number: str | None
    low_house_number: str | None
    low_house_number_sort_format: str | None
    bin: str | None
    street_attribute_indicators: str | None
    reason_code: str | None
    reason_code_qualifier: str
    latitude: float
    latitude: float
    community_district: int
    fire_division: str
    fire_battalion: str
    fire_company_type: str
    fire_company_number: int
    city_council_district: int
    community_school_district: int
    police_precinct: int
    zip_cope: str
    nta_2010: int
    nta_2020: int
    census_tract_2010: int
    census_tract_2020: int
    census_block_2010: int
    census_block_2020: int
    cdta_2020: int
    geosupport_return_code: int
    geosupport_return_code_2: int
    message: str
    message_2: str


result_dict = {
    "First Borough Name": "BROOKLYN",
    "House Number - Display Format": "93",
    "House Number - Sort Format": "000093000AA",
    "B10SC - First Borough and Street Code": "38033001020",
    "First Street Name Normalized": "SAINT JOHNS PLACE",
    "B10SC - Second Borough and Street Code": "",
    "Second Street Name Normalized": "",
    "B10SC - Third Borough and Street Code": "",
    "Third Street Name Normalized": "",
    "BOROUGH BLOCK LOT (BBL)": {
        "BOROUGH BLOCK LOT (BBL)": "3009450077",
        "Borough Code": "3",
        "Tax Block": "00945",
        "Tax Lot": "0077",
    },
    "Filler for Tax Lot Version Number": "",
    "Low House Number - Display Format": "",
    "Low House Number - Sort Format": "",
    "Building Identification Number (BIN)": "",
    "Street Attribute Indicators": "",
    "Reason Code 2": "",
    "Reason Code Qualifier 2": "",
    "Warning Code 2": "",
    "Geosupport Return Code 2 (GRC 2)": "00",
    "Message 2": "",
    "Node Number": "",
    "UNIT - SORT FORMAT": {
        "UNIT - SORT FORMAT": "",
        "Unit - Type": "",
        "Unit - Identifier": "",
    },
    "Unit - Display Format": "",
    "NIN": "",
    "Street Attribute Indicator": "",
    "Reason Code": "",
    "Reason Code Qualifier": "",
    "Warning Code": "",
    "Geosupport Return Code (GRC)": "00",
    "Message": "",
    "Number of Street Codes and Street Names in List": "",
    "List of Street Codes": [],
    "List of Street Names": [],
    "Continuous Parity Indicator/Duplicate Address Indicator": "",
    "Low House Number of Block Face": "000083000AA",
    "High House Number of Block Face": "000165000AA",
    "DCP Preferred LGC": "01",
    "Number of Cross Streets at Low Address End": "1",
    "List of Cross Streets at Low Address End": "30568001",
    "Number of Cross Streets at High Address End": "1",
    "List of Cross Streets at High Address End": "30578001",
    "LION KEY": {
        "LION KEY": "3683200020",
        "Borough Code": "3",
        "Face Code": "6832",
        "Sequence Number": "00020",
    },
    "Special Address Generated Record Flag": "",
    "Side of Street Indicator": "L",
    "Segment Length in Feet": "00781",
    "Spatial X-Y Coordinates of Address": "09909010186002",
    "Reserved for Possible Z Coordinate": "",
    "Community Development Eligibility Indicator": "I",
    "Marble Hill/Rikers Island Alternative Borough Flag": "",
    "DOT Street Light Contractor Area": "3",
    "COMMUNITY DISTRICT": {
        "COMMUNITY DISTRICT": "306",
        "Community District Borough Code": "3",
        "Community District Number": "06",
    },
    "ZIP Code": "11217",
    "Election District": "079",
    "Assembly District": "52",
    "Split Election District Flag": "",
    "Congressional District": "10",
    "State Senatorial District": "20",
    "Civil Court District": "06",
    "City Council District": "39",
    "Health Center District": "36",
    "Health Area": "4500",
    "Sanitation District": "306",
    "Sanitation Collection Scheduling Section and Subsection": "3E",
    "Sanitation Regular Collection Schedule": "WS",
    "Sanitation Recycling Collection Schedule": "ES",
    "Police Patrol Borough Command": "4",
    "Police Precinct": "078",
    "Fire Division": "11",
    "Fire Battalion": "57",
    "Fire Company Type": "L",
    "Fire Company Number": "105",
    "Community School District": "13",
    "Atomic Polygon": "402",
    "Police Patrol Borough": "BS",
    "Feature Type Code": "",
    "Segment Type Code": "U",
    "Alley or Cross Street List Flag": "",
    "Coincidence Segment Count": "1",
    "1990 Census Tract": "015900",
    "2010 Census Tract": "015900",
    "2010 Census Block": "4000",
    "2010 Census Block Suffix": "",
    "2000 Census Tract": "015900",
    "2000 Census Block": "4000",
    "2000 Census Block Suffix": "",
    "Neighborhood Tabulation Area (NTA)": "BK37",
    "DSNY Snow Priority Code": "S",
    "DSNY Organic Recycling Schedule": "ES",
    "DSNY Bulk Pickup Schedule": "EW",
    "Hurricane Evacuation Zone (HEZ)": "X",
    "Underlying Address Number for NAPs": "",
    "Underlying B7SC": "38033001",
    "Segment Identifier": "0028933",
    "Curve Flag": "",
    "List of 4 LGCs": "01",
    "BOE LGC Pointer": "1",
    "Segment Azimuth": "332",
    "Segment Orientation": "4",
    "SPATIAL COORDINATES OF SEGMENT": {
        "SPATIAL COORDINATES OF SEGMENT": "09907360186083       09914320185727",
        "X Coordinate, Low Address End": "0990736",
        "Y Coordinate, Low Address End": "0186083",
        "Z Coordinate, Low Address End": "",
        "X Coordinate, High Address End": "0991432",
        "Y Coordinate, High Address End": "0185727",
        "Z Coordinate, High Address End": "",
    },
    "SPATIAL COORDINATES OF CENTER OF CURVATURE": {
        "SPATIAL COORDINATES OF CENTER OF CURVATURE": "00000000000000",
        "X Coordinate": "0000000",
        "Y Coordinate": "0000000",
        "Z Coordinate": "",
    },
    "Radius of Circle": "",
    "Secant Location Related to Curve": "",
    "Angle to From Node - Beta Value": "",
    "Angle to To Node - Alpha Value": "",
    "From LION Node ID": "0018558",
    "To LION Node ID": "0018616",
    "LION Key for Vanity Address": "3683200020",
    "Side of Street of Vanity Address": "L",
    "Split Low House Number": "000083000AA",
    "Traffic Direction": "W",
    "Turn Restrictions": "",
    "Fraction for Curve Calculation": "",
    "Roadway Type": "1",
    "Physical ID": "0063670",
    "Generic ID": "0055770",
    "NYPD ID": "",
    "FDNY ID": "",
    "Bike Lane 2": "",
    "Bike Traffic Direction": "",
    "Street Status": "2",
    "Street Width": "30",
    "Street Width Irregular": "",
    "Bike Lane": "",
    "Federal Classification Code": "",
    "Right Of Way Type": "",
    "List of Second Set of 5 LGCs": "",
    "Legacy Segment ID": "0028933",
    "From Preferred LGCs First Set of 5": "01",
    "To Preferred LGCs First Set of 5": "01",
    "From Preferred LGCs Second Set of 5": "",
    "To Preferred LGCs Second Set of 5": "",
    "No Cross Street Calculation Flag": "",
    "Individual Segment Length": "00781",
    "NTA Name": "",
    "USPS Preferred City Name": "BROOKLYN",
    "Latitude": "40.677460",
    "Longitude": "-73.976068",
    "From Actual Segment Node ID": "0018558",
    "To Actual Segment Node ID": "0018616",
    "SPATIAL COORDINATES OF ACTUAL SEGMENT": {
        "SPATIAL COORDINATES OF ACTUAL SEGMENT": "09907360186083       09914320185727",
        "X Coordinate, Low Address End": "0990736",
        "Y Coordinate, Low Address End": "0186083",
        "Z Coordinate, Low Address End": "",
        "X Coordinate, High Address End": "0991432",
        "Y Coordinate, High Address End": "0185727",
        "Z Coordinate, High Address End": "",
    },
    "Blockface ID": "1922611465",
    "Number of Travel Lanes on the Street": "1",
    "Number of Parking Lanes on the Street": "2",
    "Number of Total Lanes on the Street": "3",
    "Street Width Maximum": "30",
    "Speed Limit": "25",
    "PUMA Code": "04005",
    "Police Sector": "78C",
    "Police Service Area": "",
    "Truck Route Type": "",
    "2020 Census Tract": "015900",
    "2020 Census Block": "3000",
    "2020 Census Block Suffix": "",
    "2020 Neighborhood Tabulation Area (NTA)": "BK0602",
    "2020 Community District Tabulation Area (CDTA)": "BK06",
    "Filler": "",
    "Return Code": "00",
    "No. of Cross Streets at High Address End": "1",
    "List of Cross Street Names at Low Address End": "6 AVENUE",
    "List of Cross Street Names at High Address End": "7 AVENUE",
    "BOE Preferred B7SC": "38033001",
    "BOE Preferred Street Name": "ST JOHNS PLACE",
    "Continuous Parity Indicator / Duplicate Address Indicator": "",
    "Low House Number of Defining Address Range": "000093000AA",
    "RPAD Self-Check Code (SCC) for BBL": "4",
    "RPAD Building Classification Code": "C1",
    "Corner Code": "",
    "Number of Existing Structures on Lot": "0001",
    "Number of Street Frontages of Lot": "01",
    "Interior Lot Flag": "",
    "Vacant Lot Flag": "",
    "Irregularly-Shaped Lot Flag": "",
    "Marble Hill/Rikers Island Alternate Borough Flag": "",
    "List of Geographic Identifiers Overflow Flag": "",
    "STROLLING KEY": {
        "STROLLING KEY": "",
        "Borough": "",
        "5-Digit Street Code of ON- Street": "",
        "Side of Street Indicator": "",
        "High House Number": "",
    },
    "Building Identification Number (BIN) of Input Address or NAP": "3019314",
    "Condominium Flag": "",
    "DOF Condominium Identification Number": "",
    "Condominium Unit ID Number": "",
    "Condominium Billing BBL": "0000000000",
    "Filler - Tax Lot Version No. Billing BBL": "",
    "Self-Check Code (SCC) of Billing BBL": "",
    "Low BBL of this Building's Condominium Units": "3009450077",
    "Filler - Tax Lot Version No. of Low BBL": "",
    "High BBL of this Building's Condominium Units": "3009450077",
    "Filler - Tax Log Version No. of High BBL": "",
    "Cooperative ID Number": "0000",
    "SBVP (SANBORN MAP IDENTIFIER)": {
        "SBVP (SANBORN MAP IDENTIFIER)": "306 039",
        "Sanborn Borough Code": "3",
        "Volume Number": "06",
        "Volume Number Suffix": "",
        "Page Number": "039",
        "Page Number Suffix": "",
    },
    "DCP Commercial Study Area": "",
    "Tax Map Number Section & Volume": "30401",
    "Reserved for Tax Map Page Number": "",
    "X-Y Coordinates of Lot Centroid": "09908880186094",
    "Business Improvement District (BID)": "",
    "TPAD BIN Status": "",
    "TPAD New BIN": "",
    "TPAD New BIN Status": "",
    "TPAD Conflict Flag": "",
    "DCP Zoning Map": "16C",
    "Internal Use": "01",
    "Number of Entries in List of Geographic Identifiers": "0001",
    "LIST OF GEOGRAPHIC IDENTIFIERS": [
        {
            "Low House Number": "93",
            "High House Number": "93",
            "Borough Code": "3",
            "5-Digit Street Code": "80330",
            "DCP-Preferred Local Group Code (LGC)": "01",
            "Building Identification Number": "3019314",
            "Side of Street Indicator": "L",
            "Geographic Identifier Entry Type Code": "",
            "TPAD BIN Status": "",
            "Street Name": "ST JOHNS PLACE",
        }
    ],
}
