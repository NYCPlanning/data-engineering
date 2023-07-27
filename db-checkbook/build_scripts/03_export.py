import boto3
import os
from datetime import date
from pathlib import Path
from dotenv import load_dotenv

BASE_BUCKET = 'edm-publishing'
_curr_file_path = Path(__file__).resolve()
LIB_DIR = _curr_file_path.parent.parent / '.library'
load_dotenv(_curr_file_path.parent.parent.parent / '.env') 


def create_metadata_folder_structure(digital_ocean_file) -> dict:
    today = date.today()
    folder_structure = f'db-checkbook/main/{today}/{digital_ocean_file}'
    dict_folder = {}
    dict_folder['Folder Structure'] = folder_structure
    return dict_folder

def create_version_text_file() -> None:
    today = date.today()
    file_version = open(_curr_file_path.parent.parent / 'output'/ 'version.txt',"w+")
    file_version.write(str(today))
    return 

def upload_final_file(final_file, digital_ocean_file) -> None:
    """ final_file: str of output csv 
        digital_ocean_file: str name of file in digital ocean to save  
    """

    s3_client = boto3.client('s3', 
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        endpoint_url=os.environ["AWS_S3_ENDPOINT"] 
        )

    folder_structure_metadata = create_metadata_folder_structure(digital_ocean_file)
    final_file_path = _curr_file_path.parent.parent / 'output' / final_file
    do_destination = f'datasets/dcp_cpdb/{digital_ocean_file}'
    s3_client.upload_file(final_file_path,
                          BASE_BUCKET, 
                          do_destination, 
                          #ExtraArgs = {"Metadata": folder_structure_metadata} 

    )
    

    return 

if __name__ == "__main__":
    print("started export ...")
    print(create_metadata_folder_structure('historical_spend.csv'))
    create_version_text_file()
    upload_final_file('temp_historical_spend.csv', 'historical_spend.csv')
    print('Finished export!')