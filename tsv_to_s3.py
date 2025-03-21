import pandas as pd
import requests
from io import BytesIO
from io import StringIO
import csv
import boto3
import gzip
#from myawslib import AwsHandler

AWS_KEY_ID = 'AAAAKEY'
AWS_SECRET_KEY = 'AAASECRET'
BUCKET_NAME = 's3-tourism-data'
REGION_NAME = 'eu-central-1'
s3 = boto3.client("s3", aws_access_key_id=AWS_KEY_ID, aws_secret_access_key=AWS_SECRET_KEY, region_name=REGION_NAME)

url_gender ="https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/tour_dem_tosex/?format=TSV&compressed=true" 
url_age = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/tour_dem_tnage/?format=TSV&compressed=true"
url_month_depature = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/tour_dem_ttmd/?format=TSV&compressed=true"
url_booking_channel = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/tour_dem_ttacb/?format=TSV&compressed=true"
url = {'tourism_gender.tsv': url_gender, 
       'tourism_age.tsv': url_age,
       'tourism_month_departure.tsv': url_month_depature, 
       'tourism_booking_channel.tsv': url_booking_channel}
# from_url_gender = requests.get(url_gender)
# from_url_age = requests.get(url_age)
# #print(from_url_gender.content)
# object_url_gender = BytesIO(from_url_gender.content)
# object_url_age = BytesIO(from_url_gender.content)

for u_name, u in url.items(): 
    from_url = requests.get(u)
    object_url = BytesIO(from_url.content)
    with gzip.open(object_url,'rt', encoding='utf-8') as gz_file:
        file_open = pd.read_csv(gz_file, delimiter = '\t')
        file_name = u_name
        csv_buffer = StringIO()
        file_open.to_csv(csv_buffer, sep ='\t', index = False, header = True)
        file_open.to_csv(file_name, sep ='\t', index = False, header = True)
        csv_temp = csv_buffer.getvalue()
        s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=csv_temp)

# with gzip.open(object_url_gender,'rt', encoding='utf-8') as gz_gender_file:
#     gender_file_open = pd.read_csv(gz_gender_file, delimiter = '\t')
#     file_name = 'tourism_gender.tsv'
#     csv_buffer = StringIO()
#     gender_file = gender_file_open.to_csv(csv_buffer,sep ='\t', index = False, header = True)
#     gender_file_open.to_csv(file_name, sep ='\t', index = False, header = True)
#     csv_temp = csv_buffer.getvalue()
#     s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=csv_temp)

    
