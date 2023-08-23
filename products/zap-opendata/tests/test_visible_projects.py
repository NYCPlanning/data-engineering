import pytest
from src.visible_projects import get_fields, get_metadata, open_data_recode


@pytest.fixture(scope="module")
def metadata_values(test_client):
    headers = test_client.request_header
    return get_metadata(headers)


@pytest.fixture(scope="module")
def fields_in_metadata(metadata_values):
    return [field["LogicalName"] for field in metadata_values]


@pytest.fixture(scope="module")
def all_fields_metadata(metadata_values):
    return {field["LogicalName"]: field for field in metadata_values}


def test_get_metadata(metadata_values):
    assert isinstance(metadata_values, list)


@pytest.mark.parametrize("dataset_name", ["dcp_projects", "dcp_projectbbls"])
def test_recode_fields(fields_in_metadata, dataset_name):
    fields_to_lookup, _ = get_fields(dataset_name)

    for field in fields_to_lookup:
        assert field in fields_in_metadata


def test_recode_fields_raise(fields_in_metadata):
    fields_to_lookup, _ = (["bad_field"], ["bad_field_rename"])

    with pytest.raises(AssertionError):
        for field in fields_to_lookup:
            assert field in fields_in_metadata


@pytest.mark.skip(reason="in-progress")
@pytest.mark.parametrize("dataset_name", ["dcp_projects", "dcp_projectbbls"])
def test_recode_values(all_fields_metadata, dataset_name):
    df_after_recode = open_data_recode(dataset_name, df_from_crm, headers)


@pytest.mark.skip(reason="may not test this yet, was used to print CRM metadata")
@pytest.mark.parametrize("dataset_name", ["dcp_projects", "dcp_projectbbls"])
def test_recode_exploration(all_fields_metadata, dataset_name):
    fields_to_lookup, _ = get_fields(dataset_name)

    for field in fields_to_lookup:
        print(f"\n\nField name: {field}")
        field_categories = all_fields_metadata[field]["OptionSet"]["Options"]
        for category in field_categories:
            print(f"CATEGORY")
            crm_code = category["Value"]
            zap_value = category["Label"]["LocalizedLabels"][0]["Label"]
            print(f"\t{crm_code}")
            print(f"\t{zap_value}")
