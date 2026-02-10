from typing import Dict

import pandas as pd
import requests
from sqlalchemy import text

OPEN_DATA = ["dcp_projects", "dcp_projectbbls"]


PICKLIST_METADATA_LINK = "https://nycdcppfs.crm9.dynamics.com/api/data/v9.1/EntityDefinitions(LogicalName='dcp_project')/Attributes/Microsoft.Dynamics.CRM.PicklistAttributeMetadata?$select=LogicalName&$expand=OptionSet"
STATUS_METADATA_LINK = "https://nycdcppfs.crm9.dynamics.com/api/data/v9.1/EntityDefinitions(LogicalName='dcp_project')/Attributes/Microsoft.Dynamics.CRM.StatusAttributeMetadata?$select=LogicalName&$expand=OptionSet"
RECODE_FIELDS = {
    "dcp_projects": [
        ("dcp_visibility", "dcp_visibility"),
        ("statuscode", "project_status"),
        ("dcp_leaddivision", "lead_division"),
        ("dcp_publicstatus", "public_status"),
        ("dcp_ulurp_nonulurp", "ulurp_non"),
        ("dcp_ceqrtype", "ceqr_type"),
        ("dcp_easeis", "eas_eis"),
        ("dcp_applicanttype", "applicant_type"),
        ("dcp_borough", "borough"),
        ("dcp_mihmappedbutnotproposed", "mih_mapped_no_res"),
    ],
    "dcp_projectbbls": ["validated_borough", "unverified_borough"],
}

CRM_CODE_PROJECT_IS_VISIBLE = "717170003"


def make_staging_table(sql_engine, dataset_name) -> None:
    source_table_name = f"{dataset_name}_crm"
    staging_table_name = f"{dataset_name}_staging"
    if dataset_name == "dcp_projects":
        statement_staging_table = """
            BEGIN;
            DROP TABLE IF EXISTS %(staging_table_name)s;
            CREATE TABLE %(staging_table_name)s as 
            (SELECT dcp_name as project_id,
                    dcp_projectname as project_name,
                    dcp_projectid as crm_project_id,
                    dcp_projectbrief as project_brief,
                    dcp_projectdescription,
                    dcp_visibility,
                    dcp_leaddivision as lead_division,
                    dcp_femafloodzonev as fema_flood_zone_v,
                    dcp_femafloodzonecoastala as fema_flood_zone_coastal,
                    dcp_wrpreviewrequired as wrp_review_required,
                    dcp_currentzoningdistrict as current_zoning_district,
                    dcp_proposedzoningdistrict as proposed_zoning_district,
                    statuscode as project_status,
                    dcp_publicstatus as public_status,
                    dcp_ulurp_nonulurp as ulurp_non,
                    dcp_actionnumbers as actions,
                    dcp_ulurpnumbers as ulurp_numbers,
                    dcp_ceqrtype as ceqr_type, 
                    dcp_ceqrnumber as ceqr_number,
                    dcp_easeis as eas_eis, 
                    _dcp_leadagencyforenvreview_value as ceqr_leadagency,
                    _dcp_applicant_customer_value as primary_applicant, 
                    dcp_applicanttype as applicant_type, 
                    dcp_borough as borough, 
                    dcp_validatedcommunitydistricts as community_district,
                    dcp_validatedcitycouncildistricts as cc_district, 
                    dcp_femafloodzonea as flood_zone_a,
                    dcp_femafloodzoneshadedx as flood_zone_shadedx, 
                    _dcp_currentmilestone_value as current_milestone, 
                    dcp_currentmilestoneactualstartdate as current_milestone_date,
                    _dcp_currentenvironmentmilestone_value as current_envmilestone, 
                    dcp_currentenvmilestoneactualstartdate as current_envmilestone_date, 
                    dcp_applicationfileddate as app_filed_date, 
                    dcp_noticeddate as noticed_date, 
                    dcp_certifiedreferred as certified_referred, 
                    dcp_approvaldate as approval_date,
                    dcp_projectcompleted as completed_date,
                    dcp_createmodifymandatoryinclusionaryhousinga as mih_flag,
                    dcp_createmodifymihareaoption1 as mih_option1,
                    dcp_createmodifymihareaoption2 as mih_option2, 
                    dcp_createmodifymihareaworkforceoption as mih_workforce,
                    dcp_createmodifymihareadeepaffordabilityoptio as mih_deepaffordability, 
                    dcp_mihmappedbutnotproposed as mih_mapped_no_res,
                    dcp_projectphase,
                    dcp_residentialsqft,
                    dcp_totalnoofdusinprojecd,
                    dcp_dcptargetcertificationdate,
                    dcp_mihdushighernumber,
                    dcp_mihduslowernumber,
                    dcp_numberofnewdwellingunits,
                    dcp_noofvoluntaryaffordabledus

                from
                    %(source_table_name)s);
                COMMIT;
            """ % {
            "source_table_name": source_table_name,
            "staging_table_name": staging_table_name,
        }
    elif dataset_name == "dcp_projectbbls":
        statement_staging_table = """
            BEGIN;
            DROP TABLE IF EXISTS %(staging_table_name)s;
            CREATE TABLE %(staging_table_name)s as 
            (SELECT split_part(replace(%(source_table_name)s.dcp_name, ' ', ''), '-', 1) as project_id,
                    %(source_table_name)s.statuscode as project_status,
                    %(source_table_name)s.dcp_bblnumber as bbl,
                    %(source_table_name)s.dcp_validatedborough as validated_borough,
                    %(source_table_name)s.dcp_validatedblock as validated_block,
                    %(source_table_name)s.dcp_validatedlot as validated_lot,
                    %(source_table_name)s.dcp_bblvalidated as validated,
                    %(source_table_name)s.dcp_bblvalidateddate as validated_date,
                    %(source_table_name)s.dcp_userinputborough as unverified_borough,
                    %(source_table_name)s.dcp_userinputblock as unverified_block,
                    %(source_table_name)s.dcp_userinputlot as unverified_lot
            FROM %(source_table_name)s);
            COMMIT;
        """ % {
            "source_table_name": source_table_name,
            "staging_table_name": staging_table_name,
        }
    else:
        statement_staging_table = f"""
            BEGIN;
            DROP TABLE IF EXISTS %{staging_table_name}s;
            CREATE TABLE %{staging_table_name}s as SELECT * FROM %{source_table_name}s;
            COMMIT;
        """
    with sql_engine.begin() as sql_conn:
        sql_conn.execute(statement=text(statement_staging_table))


def make_open_data_table(sql_engine, dataset_name) -> None:
    if dataset_name == "dcp_projects":
        statement_visible = """
            BEGIN;
            DROP TABLE IF EXISTS dcp_projects_visible;
            CREATE TABLE dcp_projects_visible as 
            (SELECT project_id,
                    project_name,
                    project_brief,
                    dcp_visibility,
                    project_status,
                    public_status,
                    ulurp_non,
                    actions,
                    ulurp_numbers,
                    ceqr_type, 
                    ceqr_number,
                    eas_eis, 
                    ceqr_leadagency,
                    primary_applicant, 
                    applicant_type, 
                    borough, 
                    community_district,
                    cc_district, 
                    flood_zone_a,
                    flood_zone_shadedx, 
                    current_milestone, 
                    current_milestone_date,
                    current_envmilestone, 
                    current_envmilestone_date, 
                    app_filed_date, 
                    noticed_date, 
                    certified_referred, 
                    approval_date,
                    completed_date,
                    mih_flag,
                    mih_option1,
                    mih_option2, 
                    mih_workforce,
                    mih_deepaffordability, 
                    mih_mapped_no_res
                from
                    dcp_projects_recoded
                where
                    dcp_visibility = 'General Public');
                COMMIT;
            """
        with sql_engine.begin() as sql_conn:
            sql_conn.execute(statement=text(statement_visible))
    elif dataset_name == "dcp_projectbbls":
        statement = """
            BEGIN;
            DROP TABLE IF EXISTS dcp_projectbbls_visible;
            CREATE TABLE dcp_projectbbls_visible as 
            (SELECT dcp_projectbbls_recoded.project_id as project_id,
                    dcp_projectbbls_recoded.bbl as bbl,
                    dcp_projectbbls_recoded.validated_borough as validated_borough,
                    dcp_projectbbls_recoded.validated_block as validated_block,
                    dcp_projectbbls_recoded.validated_lot as validated_lot,
                    dcp_projectbbls_recoded.validated as validated,
                    dcp_projectbbls_recoded.validated_date as validated_date,
                    dcp_projectbbls_recoded.unverified_borough as unverified_borough,
                    dcp_projectbbls_recoded.unverified_block as unverified_block,
                    dcp_projectbbls_recoded.unverified_lot as unverified_lot
             from dcp_projectbbls_recoded INNER JOIN dcp_projects_visible 
            on dcp_projectbbls_recoded.project_id = dcp_projects_visible.project_id);
            COMMIT;
        """
        with sql_engine.begin() as sql_conn:
            sql_conn.execute(statement=text(statement))
    else:
        raise NotImplementedError(f"Unimplemented open dataset: {dataset_name}")


def open_data_recode(name: str, data: pd.DataFrame, headers: Dict) -> pd.DataFrame:
    recoder = {}

    fields_to_lookup, fields_to_rename = get_fields(name)

    # Standardize integer representation
    print("standardize integer representation ...")
    data[fields_to_rename] = data[fields_to_rename].apply(
        func=lambda x: x.str.split(".").str[0], axis=1
    )

    # Get metadata
    print("get_metadata ...")
    metadata_values = get_metadata(headers)

    # Construct list of just fields we want to recode
    print("Construct list of fields to recode ...")
    fields_to_recode = {}
    for field in metadata_values:
        if field["LogicalName"] in fields_to_lookup:
            fields_to_recode[field["LogicalName"]] = field

    print("populate recoder ...")
    for crm_name, local_name in zip(fields_to_lookup, fields_to_rename):
        field_metadata = fields_to_recode[crm_name]
        field_recodes = {}
        print(f"for {crm_name}, {local_name} ...")
        for category in field_metadata["OptionSet"]["Options"]:
            field_recodes[str(category["Value"])] = category["Label"][
                "LocalizedLabels"
            ][0]["Label"]
        if name == "dcp_projectbbls":
            recoder["validated_borough"] = field_recodes
            recoder["unverified_borough"] = field_recodes
        elif name == "dcp_projects":
            recoder[local_name] = field_recodes

    print("replace values using recoder ...")
    data.replace(to_replace=recoder, inplace=True)

    return data


def get_fields(name) -> tuple[list, list]:
    if name == "dcp_projectbbls":
        fields_to_lookup = ["dcp_borough"]
        fields_to_rename = RECODE_FIELDS[name]
    elif name == "dcp_projects":
        fields_to_lookup = [t[0] for t in RECODE_FIELDS[name]]
        fields_to_rename = [t[1] for t in RECODE_FIELDS[name]]
    else:
        raise f"no recode written for {name}"
    return fields_to_lookup, fields_to_rename


def get_metadata(headers) -> list:
    metadata_values = []
    for link in [PICKLIST_METADATA_LINK, STATUS_METADATA_LINK]:
        res = requests.get(link, headers=headers)
        metadata_values.extend(res.json()["value"])
    return metadata_values
