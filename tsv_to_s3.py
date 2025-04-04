"""Mój skrypt"""

import gzip
import json
from io import BytesIO
import os

import boto3
import pandas as pd
import requests
import yaml


CREDENTIALS_FILE = "s3_tourism_credentials.json"
TSV_LIST_FILENAME = "url_tsv.yml"
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

print("Loading TSV files list")
with open(TSV_LIST_FILENAME, encoding="UTF-8") as url_file:
    tsv_files = yaml.safe_load(url_file)

print("Creating folder for source files")
try:
    os.mkdir(SOURCE_FOLDER)
except FileExistsError:
    pass

print("Processing TSV requests")
for tsv_file in tsv_files["sources"]:
    source_url = tsv_file["url"]
    file_name = tsv_file["filename"]
    print(f"Processing URL {source_url}")
    file_request = requests.get(source_url, timeout=60)
    remote_file = BytesIO(file_request.content)

    print("Opening compressed file")
    with gzip.open(remote_file, "rt", encoding="utf-8") as gz_file:
        print("Loading TSV to Pandas")
        df = pd.read_csv(gz_file, delimiter="\t")
        print("Converting to CSV")
        df.to_csv(f"{SOURCE_FOLDER}/{file_name}", sep="\t", index=False, header=True)
        print("Uploading to AWS")
        s3.upload_file(f"{SOURCE_FOLDER}/{file_name}", bucket_name, file_name)
