import pandas as pd
import requests
from io import BytesIO
from io import StringIO
import csv
import boto3

#from myawslib import AwsHandler

AWS_KEY_ID = 'AAAAAAAKEY' 
AWS_SECRET_KEY = 'AAAAAASECRET'
BUCKET_NAME = 's3-tourism-data'
REGION_NAME = 'eu-central-1'


URL ="https://pre-webunwto.s3.eu-west-1.amazonaws.com/s3fs-public/2024-07/unwto-all-data-download_2022.xlsx" 
from_url = requests.get(URL)
excel_file = pd.read_excel(BytesIO(from_url.content), sheet_name=None)
s3 = boto3.client("s3", aws_access_key_id=AWS_KEY_ID, aws_secret_access_key=AWS_SECRET_KEY, region_name=REGION_NAME)
#tourism_s3 = AwsHandler(BUCKET_NAME, AWS_KEY_ID, AWS_SECRET_KEY) 

#excel_file = pd.read_excel("unwto-all-world.xlsx", sheet_name=None)
unwto_inbound_tourism_regions = excel_file["Inbound Tourism-Regions"]
unwto_outbound_tourism_departures = excel_file["Outbound Tourism-Departures"]
unwto_outbound_tourism_expenditure = excel_file["Outbound Tourism-Expenditure"]
unwto_tourism_industries = excel_file["Tourism Industries"]


# unwto_inbound_tourism_regions = pd.read_excel("unwto-all-world.xlsx", sheet_name="Inbound Tourism-Regions")
# unwto_outbound_tourism_departures = pd.read_excel("unwto-all-world.xlsx", sheet_name="Outbound Tourism-Departures")
# unwto_outbound_tourism_expenditure = pd.read_excel("unwto-all-world.xlsx", sheet_name="Outbound Tourism-Expenditure")
# unwto_tourism_industries = pd.read_excel("unwto-all-world.xlsx", sheet_name="Tourism Industries")
# xls = pd.ExcelFile("unwto-all-world.xlsx")
#print(excel_file.keys())
#print (xls.sheet_names)
#df = pd.DataFrame(excel_file)
#print(df)

#print (unwto_inbound_tourism_regions)

#print (unwto_tourism_industries)#

# csv_unwto_inbound_tourism_regions = unwto_inbound_tourism_regions.to_csv("unwto_inbound_tourism_regions.csv", index = None, header = True)
# csv_unwto_outbound_tourism_departures = unwto_outbound_tourism_departures.to_csv("unwto_outbound_tourism_departures.csv", index = None, header = True)
# csv_unwto_outbound_tourism_expenditure = unwto_outbound_tourism_expenditure.to_csv("unwto_outbound_tourism_expenditure.csv", index = None, header = True)
# csv_unwto_tourism_industries = unwto_tourism_industries.to_csv("unwto_tourism_industries.csv", index = None, header = True)

unwto_frames = {
    "unwto_inbound_tourism_regions.csv": unwto_inbound_tourism_regions, 
    "unwto_outbound_tourism_departures.csv": unwto_outbound_tourism_departures,
    "unwto_outbound_tourism_expenditure.csv": unwto_outbound_tourism_expenditure, 
    "unwto_tourism_industries.csv": unwto_tourism_industries
    }

for file_name, f in unwto_frames.items():
    csv_buffer = StringIO()
    f.to_csv(csv_buffer, index=False)
    csv_temp = csv_buffer.getvalue()
    print(file_name)
    s3.put_object(Bucket=BUCKET_NAME, Key=file_name, Body=csv_temp)
   
#tourism_s3.upload_to_s3(BUCKET_NAME, csv_temp, f"{file_name}.csv")


#print(unwto_inbound_tourism_regions.columns)

