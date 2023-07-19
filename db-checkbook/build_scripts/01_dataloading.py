from dcpy.connectors.s3 import client


def do_stuff() -> None:
    print("doing stuff")
    s3_client = client()
    available_buckets = [
        bucket["Name"] for bucket in s3_client.list_buckets()["Buckets"]
    ]
    print(f"This S3 client is a {type(s3_client)}")
    print(f"This S3 client has access to buckets: {available_buckets}")

if __name__ == "__main__":
    print("started dataloading ...")
    do_stuff()