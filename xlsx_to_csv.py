"""XLSX downloader and converter with AWS exporter"""

from io import BytesIO
import json
import os

import boto3
import pandas as pd
import requests
import yaml


CREDENTIALS_FILE = "s3_tourism_credentials.json"
XSLX_LIST_FILENAME = "xlsx_sources.yaml"
SOURCE_FOLDER = "source_files"

print("Loading credentials")
with open(CREDENTIALS_FILE, encoding="UTF-8") as credentials_file:
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
with open(XSLX_LIST_FILENAME, encoding="UTF-8") as url_file:
    xlsx_files = yaml.safe_load(url_file)

print("Creating folder for source files")
try:
    os.mkdir(SOURCE_FOLDER)
except FileExistsError:
    pass

print("Processing XLSX requests")
for xlsx_file in xlsx_files:
    source_url = xlsx_file["url"]
    print(f"Processing URL {source_url}")
    file_request = requests.get(source_url, timeout=60)
    remote_file = BytesIO(file_request.content)
    print("Loading XLSX to Pandas")
    excel_file = pd.read_excel(remote_file, sheet_name=None)

    for sheet in xlsx_file["sheets"]:
        sm = sheet["sheet_name"]
        print(f"Processing sheet {sm}")
        df = excel_file[sm]
        file_name = sheet["csv_name"]
        print("Converting to CSV")
        df.to_csv(f"{SOURCE_FOLDER}/{file_name}", sep=",", index=False, header=True)
        print("Uploading to AWS")
        s3.upload_file(f"{SOURCE_FOLDER}/{file_name}", bucket_name, file_name)
