import json
import logging
import os
from typing import List

import pandas as pd
import requests

from . import CLIENT_ID, SECRET, TENANT_ID, ZAP_DOMAIN
from .client import Client

RECODE_ID_FIELDS = [
    "primary_applicant",
    "ceqr_leadagency",
    "current_milestone",
    "current_envmilestone",
]

RECODE_METADATA = {
    "primary_applicant": {
        "metadata_field_names": [
            "dcp_applicant_customer_account",
            "dcp_applicant_customer_contact",
        ],
        "metadata_keys": {
            "dcp_applicant_customer_account": (
                "name",
                "accountid",
            ),
            "dcp_applicant_customer_contact": (
                "fullname",
                "contactid",
            ),
        },
    },
    "ceqr_leadagency": {
        "metadata_field_names": [
            "dcp_leadagencyforenvreview",
        ],
        "metadata_keys": {
            "dcp_leadagencyforenvreview": (
                "name",
                "accountid",
            )
        },
    },
    "current_milestone": {
        "metadata_field_names": [
            "dcp_CurrentMilestone",
        ],
        "metadata_keys": {
            "dcp_CurrentMilestone": (
                "dcp_name",
                "dcp_projectmilestoneid",
            )
        },
    },
    "current_envmilestone": {
        "metadata_field_names": [
            "dcp_currentenvironmentmilestone",
        ],
        "metadata_keys": {
            "dcp_currentenvironmentmilestone": (
                "dcp_name",
                "dcp_projectmilestoneid",
            )
        },
    },
}


def get_headers():
    client = Client(
        zap_domain=ZAP_DOMAIN,
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        secret=SECRET,
    )
    return client.request_header


def create_logger(logger_name, file_name) -> logging.Logger:
    if not os.path.exists(".logs/"):
        os.makedirs(".logs/")
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter("%(asctime)s:%(name)s: %(message)s")
    logger.handlers = []
    file_handler = logging.FileHandler(f".logs/{file_name}")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    if not logger.hasHandlers():
        logger.addHandler(file_handler)
    return logger


class ReuseTracker:
    def __init__(self) -> None:
        self.primary_applicant = {}
        self.ceqr_leadagency = {}
        self.current_milestone = {}
        self.current_envmilestone = {}
        self.logger = create_logger("Reuse Summary Logger", "reused_summary.log")
        self.reused_recodings = {f: 0 for f in RECODE_ID_FIELDS}
        self.total_records_recoded = 0
        self.count_unseen_ID = 0

    def new_recode(self, field: str, mapping: dict):
        self.__getattribute__(field).update(mapping)

    def find_recode(self, row):
        new_ID = False
        self.total_records_recoded += 1
        for field in RECODE_ID_FIELDS:
            value = row[field]
            if value is not None:
                if value in self.__getattribute__(field).keys():
                    row[field] = self.__getattribute__(field)[value]
                    self.reused_recodings[field] += 1
                else:
                    self.count_unseen_ID += 1
                    new_ID = True
        return new_ID, row


class AuthRefresher:
    """Serve fresh headers after 401"""

    def __init__(self):
        self.headers = get_headers()
        self.logger = create_logger(
            "Authorization Refresher Logger", "auth_refresher.log"
        )

    def refresh_headers(self):
        self.headers = get_headers()
        self.logger.info("Got new headers")


def recode_id(data: pd.DataFrame, debug_rows: int = False) -> pd.DataFrame:
    """Recode unique ID's from the CRM to human-readable"""
    recode_logger = create_logger("Recode Logger", "recode_logger.log")
    auth_refresher = AuthRefresher()
    if debug_rows:
        data = data.sample(debug_rows)
        data.index = [x for x in range(data.shape[0])]
    recode_tracker = ReuseTracker()
    cleaned = data.apply(
        axis=1,
        func=recode_single_project,
        args=(auth_refresher, recode_tracker, recode_logger),
    )
    recode_tracker.logger.info(
        f"Records that had any ID value recoded: {recode_tracker.total_records_recoded} out of {cleaned.shape[0]}"
    )
    recode_tracker.logger.info(
        f"Where existing recode could be used: {recode_tracker.reused_recodings}"
    )
    recode_tracker.logger.info(
        f"Number of records with new ID for which URL had to be hit {recode_tracker.count_unseen_ID}"
    )
    return cleaned


def recode_single_project(
    row: pd.Series,
    auth: AuthRefresher,
    recode_tracker: ReuseTracker,
    logger: logging.Logger,
):
    # TODO make this faster!
    # start_time = time.time()
    additional_recode, row = recode_tracker.find_recode(row)
    if additional_recode:
        logger.debug(f"Hitting URL for row {row.name}")
    else:
        logger.debug(f"No additional recode needed for row {row.name}")
    if additional_recode:
        url = expand_url(row.crm_project_id)
        res = requests.get(url, headers=auth.headers)
        if res.status_code != 200:
            auth.refresh_headers()
            res = requests.get(url, headers=auth.headers)
            if res.status_code != 200:
                request_text = f"""
                    broken url
                        {url}
                    produced
                        {res.status_code=}
                        {res.text=}
                        {res.json()=}
                    """
                print(request_text)
                return row
        expanded_project_data = res.json()

        for field_to_recode in RECODE_ID_FIELDS:
            row[field_to_recode] = convert_to_human_readable(
                expanded=expanded_project_data,
                row=row,
                local_fieldname=field_to_recode,
                recode_tracker=recode_tracker,
                logger=logger,
            )
        # print(f"duration: {divmod(time.time() - start_time, 60)}")

    return row


def convert_to_human_readable(
    expanded: dict,
    row: pd.Series,
    local_fieldname: str,
    recode_tracker: ReuseTracker,
    logger=logging.Logger,
    metadata_field_names: List[str] = None,
    metadata_keys: dict = None,
):
    # Potential upgrade: return field instead of entire row
    id_val = row[local_fieldname]
    logger.info(
        f"Recoding record {row.name}: field {local_fieldname} has id of {id_val}"
    )
    if metadata_field_names is None:
        metadata_field_names = RECODE_METADATA[local_fieldname][
            "metadata_field_names"
        ].copy()
        metadata_keys = RECODE_METADATA[local_fieldname]["metadata_keys"]
    field_dict = None
    while metadata_field_names and field_dict is None:
        metadata_field = metadata_field_names.pop()
        field_dict = expanded[metadata_field]
    metadata_hr_key, metadata_id_key = (
        metadata_keys[metadata_field][0],
        metadata_keys[metadata_field][1],
    )
    if field_dict is None:
        if id_val is not None:
            raise Exception(
                f"data has {local_fieldname} of {id_val} but expanded of {json.dumps(expanded, indent=2)}"
            )
        return id_val
    else:
        human_readable = field_dict[metadata_hr_key]
        if (field_dict[metadata_id_key] != id_val) and (
            field_dict[metadata_hr_key] != id_val
        ):
            message = f"Mismatch between {field_dict[metadata_id_key]}/{field_dict[metadata_hr_key]} and {id_val} for field {local_fieldname}"
            logger.info(message)

        logger.info(f"assigning {human_readable} to field {local_fieldname}")

        recode_tracker.new_recode(local_fieldname, {id_val: human_readable})
    return human_readable


def expand_url(project_id):
    return f"{ZAP_DOMAIN}/api/data/v9.1/dcp_projects({project_id})?$select=_dcp_applicant_customer_value,_dcp_currentmilestone_value,dcp_name&$expand=dcp_CurrentMilestone($select=dcp_name),dcp_leadagencyforenvreview($select=name),dcp_applicant_customer_contact($select=fullname),dcp_currentenvironmentmilestone($select=dcp_name),dcp_applicant_customer_account($select=name)"
