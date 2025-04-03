"""Mój skrypt"""

import gzip
import json
from io import BytesIO

import boto3
import pandas as pd
import requests
import yaml

with open("s3_tourism_credentials.json", "r", encoding="UTF-8") as credentials_file:
    credentials = json.load(credentials_file)

aws_key_id = credentials["aws_key_id"]
aws_secret_key = credentials["aws_secret_key"]
bucket_name = credentials["bucket_name"]
region_name = credentials["region_name"]

s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_key_id,
    aws_secret_access_key=aws_secret_key,
    region_name=region_name,
)
with open("url_tsv.yml") as url_file:
    urls = yaml.safe_load(url_file)

# print(urls)

for u in urls["sources"]:
    source_url = u["url"]
    file_name = u["filename"]
    from_url = requests.get(source_url)
    object_url = BytesIO(from_url.content)
    with gzip.open(object_url, "rt", encoding="utf-8") as gz_file:
        file_open = pd.read_csv(gz_file, delimiter="\t")
        file_open.to_csv(file_name, sep="\t", index=False, header=True)
        s3.upload_file(file_name, bucket_name, file_name)
