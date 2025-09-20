import requests
import boto3
from tomlkit import datetime

API_URL = "http://104.237.2.219:9090/generate-transactions?count=1000"
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"  # Replace with the token your API expects
headers = {"Authorization": f"Bearer {TOKEN}"}

#response = requests.get(API_URL, headers=headers)
#response.raise_for_status()


s3 = boto3.client('s3')


def generate_and_upload_transactions():
    from datetime import datetime
    import json, os

    # Get DAG run timestamp from context (logical date)
    #dt_timestamp = context['ds_nodash'] + "_" + context['ts'].split("T")[1].replace(":", "").split("+")[0]
    #dt_timestamp = context['ds_nodash'] + "_" + context['ts'].split("T")[1].split(".")[0].replace(":", "")
    S3_FILENAME = f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    S3_BUCKET = "json-assignment"
    S3_PREFIX = "pos_json/"
    S3_OBJECT_KEY = f"{S3_PREFIX}{S3_FILENAME}"
    SNOWFLAKE_STAGE_PATH = f"@RETAIL_DEMO_DB.BRONZE.AWS_DEMO_STAGE_3/{S3_OBJECT_KEY}"

    try:
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()
        data = response.json()

        local_path = f"{S3_FILENAME}"
        with open(local_path, "w") as f:
            json.dump(data, f)

        s3.upload_file(local_path, S3_BUCKET, S3_OBJECT_KEY)
        print(f"Uploaded {S3_FILENAME} to s3://{S3_BUCKET}/{S3_OBJECT_KEY}")
        os.remove(local_path)

    except Exception as e:
        raise RuntimeError(f"Upload failed: {str(e)}")



generate_and_upload_transactions()