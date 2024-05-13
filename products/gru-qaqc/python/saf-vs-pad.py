from .geo_utils import g, GeosupportError, get_borocode
from .utils import psql_insert_copy
from . import Pool, engine, cpu_count
import pandas as pd
import itertools
import sys

# SAF File column mappings:
ABCEGNPX_mapping = dict(
    b5sc=[44, 49],
    lgc1=[89, 90],
    lgc2=[91, 92],
    lgc3=[93, 94],
    lgc4=[95, 96],
    llhn=[50, 56],
    lhhn=[59, 65],
    rlhn=[68, 74],
    rhhn=[77, 83],
    saf_type=[86, 86],
)

D_mapping = dict(daps_b5sc=[44, 49], lhn=[50, 56], hhn=[59, 65], regular_b5sc=[68, 73])

S_mapping = dict(
    b5sc=[44, 49],
    lgc1=[89, 90],
    lgc2=[91, 92],
    lgc3=[93, 94],
    lgc4=[95, 96],
    hn_basic=[50, 56],
    hn_suffix=[58, 65],
    high_alphabetic_hn_suffix=[75, 75],
)

OV_mapping = dict(
    b5sc=[44, 49],
    lgc1=[89, 90],
    lgc2=[91, 92],
    lgc3=[93, 94],
    lgc4=[95, 96],
    lhn=[50, 56],
    hhn=[68, 83],
)

# Expansion function for each SAF types:
# not sure what to do when hn == '0', leave them in for now


def ABCEGNPX_expand(line: dict) -> list:
    if line["saf_type"] in ["A", "B", "C", "E", "P"]:
        b7sc = [
            line["b5sc"] + line[f"lgc{i}"]
            for i in range(1, 5)
            if line[f"lgc{i}"] not in ["", "0"]
        ]
        hn = [line[i] for i in line.keys() if "hn" in i and line[i] not in ["", "0"]]
        return list(itertools.product(b7sc, hn))
    elif line["saf_type"] in ["G", "N", "X"]:
        return [(line["b5sc"] + line["lgc1"], "")]
    else:
        return []


# basically the same as above


def OV_expand(line: dict) -> list:
    b7sc = [
        line["b5sc"] + line[f"lgc{i}"]
        for i in range(1, 5)
        if line[f"lgc{i}"] not in ["", "0"]
    ]
    hn = [line[i] for i in line.keys() if "hn" in i and line[i] not in ["", "0"]]
    return list(itertools.product(b7sc, hn))


# get unique combination of hn_basic + suffixes


def S_expand(line: dict) -> list:
    b7sc = [
        line["b5sc"] + line[f"lgc{i}"]
        for i in range(1, 5)
        if line[f"lgc{i}"] not in ["", "0"]
    ]
    hn = [
        line["hn_basic"] + line[i]
        for i in line.keys()
        if "suffix" in i and line[i] != ""
    ]
    return list(itertools.product(b7sc, hn))


def D_expand(line: dict) -> list:
    b7sc = [line[i] + "??" for i in line.keys() if "b5sc" in i]
    hn = [line[i] for i in line.keys() if "hn" in i and line[i] not in ["", "0"]]
    return list(itertools.product(b7sc, hn))


# Function that converts a line from a SAF file
# to a dictionary using given column mapping lookup


def parse_line(mapping: dict, line: str) -> dict:
    parsed_line = {}
    for key, value in mapping.items():
        parsed_line[key] = line[mapping[key][0] - 1 : mapping[key][1]].strip()
    return parsed_line


def convert_to_sname(b7sc: str) -> str:
    # if ? in b7sc, we will need to get the street name by
    # passing b5sc to function D, this is specific
    # for SAF type => D
    try:
        if "?" in b7sc:
            sname = g["D"](B5SC=b7sc[:-2]).get("First Street Name Normalized", "")
        else:
            sname = g["DG"](B7SC=b7sc).get("First Street Name Normalized", "")
    except:
        sname = ""
        print(f"invalid b7sc: {b7sc}")
    return sname


def geocode(inputs: tuple) -> dict:
    # The main geocoding function
    # Given b7sc and house number, run through given function
    b7sc, hnum = inputs[0]
    function, roadbed = inputs[1]
    sname = convert_to_sname(b7sc)
    borough = get_borocode(b7sc)

    hnum = str("" if hnum is None else hnum)
    sname = str("" if sname is None else sname)
    borough = str("" if borough is None else borough)
    try:
        if function == "1A":
            # 1A doesn't have roadbed switch
            geo = g[function](house_number=hnum, street_name=sname, borough=borough)
        geo = g[function](
            house_number=hnum,
            street_name=sname,
            borough=borough,
            roadbed_request_switch=roadbed,
        )
    except GeosupportError as e:
        geo = e.result

    geo = geo_parser(geo)

    geo.update(dict(hnum=hnum, sname=sname, borough=borough, b7sc=b7sc))
    return geo


def geo_parser(geo: dict) -> dict:
    return dict(
        geo_grc=geo.get("Geosupport Return Code (GRC)", ""),
        geo_reason_code=geo.get("Reason Code", ""),
        geo_message=geo.get("Message", "msg err"),
    )


def create_inputs(path: str, mapping: dict, expand) -> list:
    with open(path, "r") as f:
        file_list = f.read().split("\n")
    filtered = list(filter(lambda x: x.strip() != "", file_list))
    processed = list(map(lambda x: expand(parse_line(mapping, x)), filtered))
    return sum(processed, [])


if __name__ == "__main__":
    INPUT_TYPE = sys.argv[1]  # rb or gen
    FUNCTION = sys.argv[2]  # '1A' '1' '1R'
    ROADBED = ""  # Default is blank for road bed switch
    PATH = ".library/datasets/dcp_saf/"

    # Function 1R is function 1 with road bed switch on
    if FUNCTION == "1R":
        ROADBED = "R"
        FUNCTION = "1"

    if INPUT_TYPE == "rb":
        FULL_INPUT_TYPE = "Roadbed"
    elif INPUT_TYPE == "gen":
        FULL_INPUT_TYPE = "Generic"
    else:
        FULL_INPUT_TYPE = ""

    print(INPUT_TYPE, FUNCTION)

    print(f"Creating inputs from {FULL_INPUT_TYPE}ABCEGNPX.txt")
    ABCEGNPX = create_inputs(
        path=f"{PATH}{FULL_INPUT_TYPE}ABCEGNPX.txt",
        mapping=ABCEGNPX_mapping,
        expand=ABCEGNPX_expand,
    )

    print(f"Creating inputs from {FULL_INPUT_TYPE}D.txt")
    D = create_inputs(
        path=f"{PATH}{FULL_INPUT_TYPE}D.txt", mapping=D_mapping, expand=D_expand
    )

    print(f"Creating inputs from {FULL_INPUT_TYPE}S.txt")
    S = create_inputs(
        path=f"{PATH}{FULL_INPUT_TYPE}S.txt", mapping=S_mapping, expand=S_expand
    )

    print(f"Creating inputs from {FULL_INPUT_TYPE}OV.txt")
    OV = create_inputs(
        path=f"{PATH}{FULL_INPUT_TYPE}OV.txt", mapping=OV_mapping, expand=OV_expand
    )

    combined = list(itertools.product(ABCEGNPX + D + S + OV, [(FUNCTION, ROADBED)]))

    print(len(combined))

    # Geocode
    with Pool(processes=cpu_count()) as pool:
        it = pool.map(geocode, combined, 10000)

    # Return files
    df = pd.DataFrame(it)
    print("Results head:\n", df.head())

    # Export QAQC results
    df.to_sql(
        f"saf_{INPUT_TYPE}_{FUNCTION+ROADBED}_pad".lower(),
        con=engine,
        if_exists="replace",
        index=False,
        method=psql_insert_copy,
    )
