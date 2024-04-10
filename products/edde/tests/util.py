races = ["anh", "wnh", "bnh", "onh", "hsp"]

age_buckets = ["PopU16", "P16t64", "P65pl"]


def add_counts(columns):
    return [f"{c}-count" for c in columns]


race_counts = add_counts(races)
age_bucket_counts = add_counts(age_buckets)
