import json
import boto3

s3 = boto3.client("s3")

def lambda_handler(event, context):

    print("LOADER LAMBDA TRIGGERED")

    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    print(f"Bucket: {bucket}")
    print(f"Key: {key}")

    obj = s3.get_object(
        Bucket=bucket,
        Key=key
    )

    content = obj["Body"].read().decode("utf-8")

    records = json.loads(content)

    print(f"Records found: {len(records)}")

    return {
        "status": "SUCCESS",
        "records_found": len(records)
    }
