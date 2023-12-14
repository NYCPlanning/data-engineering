"""A first pass at metadata we'd sync to socrata.

Data format:
`mih_test.metadata` entry is in the format expected by their publishing API.
`mih_test.columns` is as well, but must be passed to a different endpoint.
(so while they're both metadata, we have to separate them.)

This should eventually be generated from pure DCP product metadata, as many of
the fields, especially those at column level, would be quite helpful at all
stages of the build.

FYI, the mih_test dataset is private, and will remain so.
"""

datasets = {
    "mih_test": {
        "four_four": "p6cc-cxkc",  # A `four_four` is the ID for a socrata dataset
        "attachments": ["nycmih_metadata.pdf"],
        "columns": [
            {
                "display_name": "the_geom",
                "datatype_name": "multipolygon",
                "description": "Geometry type, changed",
                "field_name": "the_geom",
            },
            {
                "display_name": "Boro",
                "datatype_name": "text",
                "description": "The NYC Borough where the Mandatory Inclusionary Housing (MIH) area is mapped. The boro code is a single digit identifier, indicating a particular borough (1 = Manhattan, 2 = Bronx, 3 = Brooklyn, 4 = Queens, 5 = Staten Island).",
                "field_name": "boro",
            },
            {
                "display_name": "Status",
                "datatype_name": "text",
                "description": "Status of the ULURP text amendment application (Adopted = final approval, Pipeline = awaiting approval)",
                "field_name": "status",
            },
            {
                "display_name": "Project Name",
                "datatype_name": "text",
                "description": "Project Name of the Mandatory Inclusionary Housing (MIH) area.",
                "field_name": "projectnam",
            },
            {
                "display_name": "Date Adopted",
                "datatype_name": "calendar_date",
                "description": "The date the ULURP text amendment application was adopted.",
                "field_name": "dateadopte",
            },
            {
                "display_name": "ZR_ULURP Number",
                "datatype_name": "text",
                "description": "The ULURP number of the ULURP text amendment application.",
                "field_name": "zr_ulurpno",
            },
            {
                "display_name": "ZR_Map",
                "datatype_name": "text",
                "description": "The map number referenced in Appendix F of the Zoning Resolution for the mapped Mandatory Inclusionary Housing (MIH) area.",
                "field_name": "zr_map",
            },
            {
                "display_name": "CD",
                "datatype_name": "text",
                "description": "Community District where the Mandatory Inclusionary Housing (MIH) area is mapped.",
                "field_name": "cd",
            },
            {
                "display_name": "MIH_Option",
                "datatype_name": "text",
                "description": "Level of affordability and options applied to the mapped Mandatory Inclusionary Housing (MIH) area.",
                "field_name": "mih_option",
            },
            {
                "display_name": "Zoning_Map",
                "datatype_name": "text",
                "description": "Zoning Map index number where the Mandatory Inclusionary Housing (MIH) area is mapped.",
                "field_name": "zoning_map",
            },
            {
                "display_name": "Project_ID",
                "datatype_name": "text",
                "description": "Zoning Application Portal (ZAP) project id. ZAP is a DCP project tracking database.",
                "field_name": "project_id",
            },
            {
                "display_name": "Shape_Length",
                "datatype_name": "number",
                "description": "Length of feature in feet.",
                "field_name": "shape_leng",
            },
            {
                "display_name": "Shape_Area",
                "datatype_name": "number",
                "description": "Area of feature in internal squared feet.",
                "field_name": "shape_area",
            },
        ],
        "metadata": {
            "name": "MIH_Test",
            "resourceName": "MIH_TEST",
            "tags": ["housing", "house", "building", "development"],
            "privateMetadata": {
                "custom_fields": {
                    "Legislative Compliance": {
                        "Removed Records?": "Yes",
                        "Original Scheduled Publication Date": "whenver",
                        "Most Recent Scheduled Publication Date": "whenver a while ago",
                        "Has Data Dictionary?": "Yes",
                        "Geocoded?": "Yes",
                        "Follows Template?": "Yes",
                        "Externally Updated- When?": "now",
                        "External Frequency (LL 110/2015)": "Historical",
                        "Exists Externally? (LL 110/2015)": "Yes",
                        "DoITT Geocoded- How?": "Manually",
                        "DoITT Geocoded- Fields?": "All of 'em",
                        "Dataset from the Open Data Plan?": "No",
                        "Contains Address?": "Yes",
                        "Can Dataset Feasibly Be Automated?": "Yes",
                        "Address Layout?": "Parsed",
                    }
                },
                "contactEmail": "a@a.com",
            },
            "metadata": {
                "rowLabel": "A Mandatorily Included House",
                "custom_fields": {
                    "Update": {
                        "Update Frequency Details": "never",
                        "Update Frequency": "As needed",
                        "Date Made Public": "never",
                        "Data Change Frequency": "always",
                        "Automation": "Yes",
                    }
                },
                "availableDisplayTypes": ["table", "fatrow", "page"],
            },
            "licenseId": "ODC_BY",
            "license": {
                "termsLink": "http://opendatacommons.org/licenses/by/1.0/",
                "name": "Open Data Commons Attribution License",
            },
            "description": "Description for Mandatory Inclusionary Housing",
            "category": "Housing & Development",
            "attribution": "DCP",
        },
    }
}
