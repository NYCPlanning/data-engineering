from pandas import DataFrame


def truncate_to_single_row(ds_id, df: DataFrame):
    return df[0:1]


def json_key_upcase_preprocessor(_, json_rec: dict):
    def _upcase_keys(d):
        return {key.upper(): value for key, value in d.items()}

    return DataFrame([_upcase_keys(r) for r in json_rec["records"]])
