import pandas as pd
import requests
from io import BytesIO
import boto3
import json
import yaml

CREDENTIALS_FILE = "s3_tourism_credentials.json"
XSLX_LIST_FILENAME = "xlsx_sources.yaml"

print("Loading credentials")
with open(CREDENTIALS_FILE, "r") as credentials_file:
    credentials = json.load(credentials_file)

aws_key_id = credentials["aws_key_id"]
aws_secret_key = credentials["aws_secret_key"]
bucket_name = credentials["bucket_name"]
region_name = credentials["region_name"]

print("Connecting to AWS")
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_key_id,
    aws_secret_access_key=aws_secret_key,
    region_name=region_name,
)

print("Loading XLSX files list")
with open(XSLX_LIST_FILENAME) as url_file:
    xlsx_files = yaml.safe_load(url_file)

print("Processing XLSX requests")
for xlsx_file in xlsx_files:
    source_url = xlsx_file["url"]
    print(f"Processing URL {source_url}")
    file_request = requests.get(source_url)
    remote_file = BytesIO(file_request.content)
    print("Loading XLSX to Pandas")
    excel_file = pd.read_excel(remote_file, sheet_name=None)

    for sheet in xlsx_file["sheets"]:
        sm = sheet["sheet_name"]
        print(f"Processing sheet {sm}")
        df = excel_file[sm]
        file_name = sheet["csv_name"]
        print("Converting to CSV")
        df.to_csv(file_name, sep=",", index=False, header=True)
        print("Uploading to AWS")
        s3.upload_file(file_name, bucket_name, file_name)
