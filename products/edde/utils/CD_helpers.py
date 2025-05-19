from dcpy.utils.logging import logger
from functools import cache

from ingest import ingestion_helpers


@cache
def _get_cd_puma_crosswalk() -> dict[str, str]:
    """Get a map of community district keys to approximate PUMAS
    e.g. BX3 -> 04263
    """
    logger.info("loading cd->puma crosswalk")
    cw = ingestion_helpers.load_data("dcp_population_cd_puma_crosswalk_2020")
    cw["puma_code"] = cw["puma_code"].apply(lambda c: f"0{c}")
    cw["dist_key"] = cw["borough_code"] + cw["community_district_num"]
    return cw.set_index("dist_key")["puma_code"].to_dict()


def community_district_to_puma(
    borough_abbrev, comm_dist_num: str | int, ignore_errors=False
):
    if not ignore_errors:
        assert len(borough_abbrev) == 2, "Abbreviated borough expected, e.g. BX"
        assert len(str(comm_dist_num)) <= 2, (
            f"Community District number should be two chars or less. got {comm_dist_num}"
        )
    return _get_cd_puma_crosswalk().get(f"{borough_abbrev}{comm_dist_num}")


def get_borough_num_mapper():
    return {"1": "MN", "2": "BX", "3": "BK", "4": "QN", "5": "SI"}
