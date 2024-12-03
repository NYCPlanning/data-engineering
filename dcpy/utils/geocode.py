from geosupport import Geosupport
from multiprocessing import Pool, cpu_count
from shapely import Point
import pandas as pd
from pydantic import BaseModel
from typing import Literal, Callable, TypeAlias

from dcpy.models import geocode


# GEOSUPPORT - map more 1-1 with underlying functionality
g = Geosupport()


def function1(
    *,
    borough: int | str | None = None,
    house_number: str,
    street_name: str,
    snl: int = 32,
    street_name_normalization: bool = False,
    zip_code: int | str | None = None,
    roadbed_request_switch: bool = False,
    browse_flag: Literal["P", "F", "R"] | None = None,
):
    """
    Function 1 processes an input address or input Non-Addressable Place name (NAP) (see Chapter III.6).  When called using two work areas, Function 1 returns information about the blockface containing the input address or NAP.  This information includes the cross streets at the two intersections delimiting the blockface, and a set of geographic district identifiers including ZIP code, census tract and community district.  Function 1 may be called with the Extended Mode Switch.

    Input: Address or Non-Addressable Place name (NAP)
    Output: Block face-level data - Standardized Street Name and Street Code, Address Range, List of Cross Streets, ZIP Code, Community District, Health Area, Health Center District, 1990 Census Tract, 2010 Census Tract and block, Fire Engine or Ladder Company, School District, Police Precinct, Police Patrol Borough, XY Coordinates (based on the State Plane Coordinate System), Hurricane Evacuation Zone.
    Extended: USPS Preferred City Name, Latitude, Longitude
    Modes: regular, extended

    Inputs
    ========================================
    borough_code (or zip_code) - Required. (ZIP Code may be used instead of Borough Code)
    house_number - Required for address input except free-form addresses (see Chapter V.3). Typically not used for NAP input (see Chapter III.6).
    street_name - Required.
    snl - Optional; default is 32. See Chapter III.2.
    street_name_normalization flag - Optional; default (blank) requests sort format. See Chapter III.3.
    cross_street_names flag - Optional
    zip_code - Optional; may be used instead of Borough Code, or to identify a DAPS. See Chapter III.6 and Chapter V.6.
    roadbed_request_switch - Optional; default (blank) requests generic information.
    browse_flag - Optional; may be used to select output street name / code. Default (blank) requests use of input street name / code. See Chapter III.8

    Reference
    ========================================
    https://nycplanning.github.io/Geosupport-UPG/appendices/appendix01/#function-1
    https://nycplanning.github.io/Geosupport-UPG/appendices/appendix13/#work-area-2-cow-functions-1-1e
    """
    if borough and zip_code:
        raise ValueError("Only borough_code or zip_code may be supplied")
    res = g["1"](kwargs_dict=locals())
    return geocode.Result1(**res)


def function1a(
    *,
    borough: int | str | None = None,
    house_number: str,
    street_name: str,
    snl: int = 32,
    street_name_normalization: bool = False,
    zip_code: int | str | None = None,
    browse_flag: Literal["P", "F", "R"] | None = None,
    long_work_area_2: bool = False,
    tpad: bool = False,
):
    """
    Function 1A processes an input address or input NAP.  When successfully called using two work areas, it returns information about the tax lot and the building (if any) identified by the input address or NAP. See Chapter VI and particularly Chapters VI.6.

    The information that is returned consists of information about the tax lot and the building (if any) identified by the input address or NAP. This information includes the Borough, Block, and, Lot (BBL), which is the Department of Finance's (DOF) identifier for the tax lot; the DOF building class code; the number of buildings on the lot; the number of street frontages of the lot; a flag indicating whether the lot is a condominium; the Building Identification Number (BIN) (see Chapter VI.3) of the building identified by the input address, if any; and the Business Improvement District (BID) if the property is in such a district. Function 1A can be called with the long option. The regular mode includes a List of Geographic Identifiers (LGI) for the tax lot, including address ranges, BINs and street frontages. The long mode includes, instead of the LGI, a List of BINs for all the buildings in the tax lot.

    Function 1A normally returns information that is updated on a quarterly basis. Users may request more up-to-date information on new buildings and demolitions using the TPAD Request Switch. The TPAD information will include the status of new building construction and/or demolition. See Chapter VI.11.

    The regular, long, and extended modes for Function 1A are identical to those for Function BL. Function 1A enables the user to retrieve this information by address, while Function BL enables retrieval by BBL.

    Input: Address
    Output: Tax lot - and building-level data - Standardized Street Name and Street Code, Tax Block and Lot, Alternative Addresses for Lot, Building Identification Number (BIN), RPAD Building Class, Interior Lot Flag, Vacant Lot Flag, Irregularly-Shaped Lot Flag, Corner Code, Business Improvement District (BID), Latitude, Longitude.
    Modes: regular, extended, long, long+tpad

    Inputs
    ========================================
    borough_code (or ZIP Code) - Required. (ZIP Code may be used instead of Borough Code)
    house_number - Required for address input except free-form addresses (see Chapter V.3). Typically not used for NAP input (see Chapter III.6).
    street_name - Required.
    snl - Optional; default is 32. See Chapter III.2.
    street_name_normalization flag - Optional; default (blank) requests sort format. SeeChapter III.3.
    zip_code - Optional; may be used instead of Borough Code, or to identify a DAPS. See Chapter III.6 andChapter V.6.
    browse_flag - Optional; may be used to select output street name / code. Default (blank) requests use of input street name / code. Chapter III.8
    long_work_area_2 flag - Optional; default (blank) is regular WA2. See Chapters II.5 and VI.6.
    tpad - Optional; may be used to request Transitional PAD information. See Chapter VI.11

    Reference
    ========================================
    https://nycplanning.github.io/Geosupport-UPG/appendices/appendix01/#function-1a
    https://nycplanning.github.io/Geosupport-UPG/appendices/appendix13/#work-area-2-cow-functions-1a-bl-bn
    """
    if borough and zip_code:
        raise ValueError("Only borough_code or zip_code may be supplied")
    res = g["1A"](kwargs_dict=locals())
    return geocode.Result1A(**res)


def function1b(
    *,
    borough: int | str | None = None,
    house_number: str,
    street_name: str,
    snl: int = 32,
    street_name_normalization: bool = False,
    zip_code: int | str | None = None,
    browse_flag: Literal["P", "F", "R"] | None = None,
):
    """
    Function 1B processes an input address or input Non-Addressable Place name (NAP) (see Chapter III.6).  Function 1B returns information about the blockface as well as information about the tax lot and the building (if any) identified by the input address or NAP. The information that is returned includes the cross streets at the two intersections delimiting the blockface, and a set of geographic district identifiers including ZIP code, census tract and community district. Information about the tax lot and the building (if any) identified by the input address or NAP is also returned. This information includes the Borough, Block, and, Lot (BBL), which is the Department of Finance's (DOF) identifier for the tax lot; the DOF building class code; the number of buildings on the lot; the number of street frontages of the lot; a flag indicating whether the lot is a condominium; and the Building Identification Number (BIN) (see Chapter VI.3) of the building identified by the input address

    Input: Address or NAP
    Output: Same as for Function 1E + Property Level Information from Function 1A + Street Names for Cross Streets and Address Lists
    Modes: regular

    Inputs
    ========================================
    borough_code (or zip_code) - Required. (ZIP Code may be used instead of Borough Code)
    house_number - Required for address input except free-form addresses (see Chapter V.3). Typically not used for NAP input (see Chapter III.6).
    street_name - Required.
    snl - Optional; default is 32. See Chapter III.2.
    street_name_normalization flag - Optional; default (blank) requests sort format. See Chapter III.3.
    zip_code - Optional; may be used instead of Borough Code, or to identify a DAPS. See Chapter III.6 and Chapter V.6.
    browse_flag - Optional; may be used to select output street name / code. Default (blank) requests use of input street name / code. See Chapter III.8

    Reference
    ========================================
    https://nycplanning.github.io/Geosupport-UPG/appendices/appendix01/#function-1b
    https://nycplanning.github.io/Geosupport-UPG/appendices/appendix13/#work-area-2-cow-function-1b
    """
    if borough and zip_code:
        raise ValueError("Only borough_code or zip_code may be supplied")
    res = g["1B"](kwargs_dict=locals())
    for key in res:
        if key.startswith("Continu"):
            print(key)
            print(res[key])
    return geocode.Result1B(**res)


def function1e(
    *,
    borough: int | str | None = None,
    house_number: str,
    street_name: str,
    snl: int = 32,
    street_name_normalization: bool = False,
    cross_street_names: bool = False,
    zip_code: int | str | None = None,
    roadbed_request_switch: bool = False,
    browse_flag: Literal["P", "F", "R"] | None = None,
):
    """
    Function 1E processes an input address or input NAP. It returns the same WA2 information that is returned by Function 1, and additionally, it returns a set of political districts, including Election, State Assembly and Senate, City Council and Congressional Districts.

    Input: Address or NAP
    Output: Same as for Function 1 + Political Geography (Election District, Assembly District, Congressional District, City Council District, Municipal Court District and State Senatorial District)
    Modes: regular, extended

    Inputs
    ========================================
    borough_code (or zip_code) - Required. (ZIP Code may be used instead of Borough Code)
    house_number - Required for address input except free-form addresses (see Chapter V.3). Typically not used for NAP input (see Chapter III.6).
    street_name - Required.
    snl - Optional; default is 32. See Chapter III.2.
    street_name_normalization flag - Optional; default (blank) requests sort format. See Chapter III.3.
    cross_street_names Flag - Optional
    zip_code - Optional; may be used instead of Borough Code, or to identify a DAPS. See Chapter III.6 and Chapter V.6.
    roadbed_request_switch - Optional; default (blank) requests generic information.
    browse_flag - Optional; may be used to select output street name / code. Default (blank) requests use of input street name / code. See Chapter III.8

    Reference
    ========================================
    https://nycplanning.github.io/Geosupport-UPG/appendices/appendix01/#function-1e
    https://nycplanning.github.io/Geosupport-UPG/appendices/appendix13/#work-area-2-cow-functions-1-1e
    """
    if borough and zip_code:
        raise ValueError("Only borough_code or zip_code may be supplied")
    res = g["1E"](kwargs_dict=locals())
    return geocode.Result1E(**res)


def function1n(
    *,
    borough: int | str | None = None,
    street_name: str,
    snl: int = 32,
    street_name_normalization: bool = False,
    zip_code: int | str | None = None,
    browse_flag: Literal["P", "F", "R"] | None = None,
):
    """
    1N (street_name_to_street_code, get_street_code)
    ========================================
    Function 1N is used to normalize a street name and obtain its street code.  Functions 1, 1A and 1E can do this also, but those functions require an input house number. The purpose of Function 1N is to provide a way to process a street name alone, without a house number. Note that since the input to Function 1N is not a specific location along a street, Function 1N does not perform local street name validation.

    Input: Street Name
    Output: Standardized Street Name and Street Code
    Modes:

    Inputs
    ========================================
    borough_code (or ZIP Code) - Required.
    street_name - Required. (Note: Street code input is not permitted for Function 1N.)
    snl - Optional; default is 32. See Chapter III.2.
    zip_code - Optional; may be used instead of Borough Code, or to identify a DAPS. See Chapter III.6 and Chapter V.6.
    street_name_normalization Flag - Optional; default (blank) requests sort format. See Chapter III.3.
    browse_flag - Optional; may be used to select output street name / code. Default (blank) requests use of input street name / code. See Chapter III.8

    Reference
    ========================================
    https://nycplanning.github.io/Geosupport-UPG/appendices/appendix01/#function-1n
    """
    if borough and zip_code:
        raise ValueError("Only borough_code or zip_code may be supplied")
    res = g["1N"](kwargs_dict=locals())
    return  # geocode.BlockFace(**res)


# GEOCODE - How I actually want to interact with i

BoroughCode: TypeAlias = Literal[1, 2, 3, 4, 5]
Borough: TypeAlias = Literal[
    "Manhattan", "Bronx", "Brooklyn", "Queens", "Staten Island"
]


class StreetAddress(BaseModel):
    borough: Borough | BoroughCode
    street_name: str
    house_number: str  # TODO - allow coercion from int


class NonAddressPlace(BaseModel):
    borough: Borough | BoroughCode
    name: str


AddressInput: TypeAlias = StreetAddress | NonAddressPlace


def geocode_df(
    df: pd.DataFrame,
    *,
    borough_column: str = "borough",
    street_name_column: str = "street_name",
    house_number_column: str = "house_number",
    zip_code_column: str | None = None,
    snl: int = 32,
    street_name_normalization: bool = False,
    browse_flag: bool = False,
) -> pd.DataFrame:
    # todo - definitely need some sort of caching for individual functions
    # unless we can improve inner workings of geosupport, there are cases where
    # we need to go BBL -> geographic identifiers / 5-digit street code -> address -> 1a
    # though performance test for sure - maybe minimal benefit
    data_records = df.to_dict("records")

    def func(dict):
        if zip_code_column:
            borough = None
            zip_code = dict[zip_code_column]
        else:
            borough = dict[borough_column]
            zip_code = None
        return function1b(
            borough=borough,
            zip_code=zip_code,
            street_name=dict[street_name_column],
            house_number=dict[house_number_column],
            snl=snl,
            street_name_normalization=street_name_normalization,
            browse_flag=browse_flag,
        )

    # Multiprocess
    with Pool(processes=cpu_count()) as pool:
        geocoded_records = pool.map(func, data_records, 10000)

    return pd.DataFrame(geocoded_records)
