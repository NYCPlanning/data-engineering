import pandas as pd
from src.recode_id import recode_id

test_data_input = pd.DataFrame.from_dict(
    {
        "crm_project_id": [
            "046f7ed3-6197-e911-a98a-001dd83080ab",
            "046f7ed3-24-more-text",
        ],
        "primary_applicant": [
            "applicant_1",
            "applicant_2",
        ],
        "ceqr_leadagency": [
            "agency_1",
            "agency_2",
        ],
        "current_milestone": [
            "milestone_1",
            "milestone_2",
        ],
        "current_envmilestone": [
            "envmilestone_1",
            "envmilestone_2",
        ],
    }
)

recoded_data_expected = pd.DataFrame.from_dict(
    {
        "crm_project_id": [
            "046f7ed3-6197-e911-a98a-001dd83080ab",
            "046f7ed3-24-more-text",
        ],
        "primary_applicant": [
            "624 Morris B, LLC",
            "applicant_2",
        ],
        "ceqr_leadagency": [
            "DCP",
            "agency_2",
        ],
        "current_milestone": [
            "EAS - Project Completed  ",
            "milestone_2",
        ],
        "current_envmilestone": [
            "EAS - Review Filed EAS  ",
            "envmilestone_2",
        ],
    }
)


def test_recode_id():
    recoded_data_actual = recode_id(test_data_input)
    pd.testing.assert_frame_equal(recoded_data_actual, recoded_data_expected)
