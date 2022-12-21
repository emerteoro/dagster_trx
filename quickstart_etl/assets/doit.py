import base64
from io import BytesIO
import io
import os
import json
import pandas as pd
import requests
import boto3

from dagster import asset

@asset(group_name="doit", compute_kind="Challenge")
def doit():
    AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    URL = 'https://corgis-edu.github.io/corgis/datasets/json/airlines/airlines.json'
    data = json.loads(requests.get(URL).text)
    # Flattening JSON data
    df = pd.json_normalize(data,sep=' ')

    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False, doublequote=True, errors='ignore',sep="|")
        response = s3_client.put_object(
            Bucket=AWS_S3_BUCKET, Key="airlines.csv", Body=csv_buffer.getvalue()
        )

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    
    if status == 200:
      print(f"Successful S3 put_object response. Status - {status}")
    else:
      print(f"Unsuccessful S3 put_object response. Status - {status}")
