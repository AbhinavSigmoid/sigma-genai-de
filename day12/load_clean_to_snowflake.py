from dotenv import load_dotenv
import os
import json
import boto3
import snowflake.connector

# Load env vars
load_dotenv("lab/.env")

BUCKET = os.getenv("SIGMA_S3_BUCKET")

# AWS clients
s3 = boto3.client("s3")

# --------------------------------------------------
# Find latest clean file
# --------------------------------------------------

resp = s3.list_objects_v2(
    Bucket=BUCKET,
    Prefix="clean/"
)

files = sorted(
    resp.get("Contents", []),
    key=lambda x: x["LastModified"],
    reverse=True
)

if not files:
    raise Exception("No clean files found")

latest_file = files[0]["Key"]

print(f"Latest file: {latest_file}")

# --------------------------------------------------
# Read JSON from S3
# --------------------------------------------------

obj = s3.get_object(
    Bucket=BUCKET,
    Key=latest_file
)

records = json.loads(
    obj["Body"].read().decode("utf-8")
)

print(f"Records found: {len(records)}")

# --------------------------------------------------
# Connect Snowflake
# --------------------------------------------------

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

cur = conn.cursor()

# --------------------------------------------------
# Insert Records
# --------------------------------------------------

loaded = 0

for rec in records:

    transaction_id = rec.get("transaction_id")
    merchant_name = rec.get("merchant_name")
    amount = float(rec.get("amount", 0))

    cur.execute(
        """
        INSERT INTO TRANSACTIONS
        (
            transaction_id,
            merchant_name,
            amount,
            transaction_ts
        )
        VALUES
        (
            %s,
            %s,
            %s,
            CURRENT_TIMESTAMP()
        )
        """,
        (
            transaction_id,
            merchant_name,
            amount
        )
    )

    loaded += 1

conn.commit()

print(f"Loaded {loaded} records into Snowflake")

# --------------------------------------------------
# Verify
# --------------------------------------------------

cur.execute(
    "SELECT COUNT(*) FROM TRANSACTIONS"
)

count = cur.fetchone()[0]

print(f"Total rows in TRANSACTIONS: {count}")

cur.close()
conn.close()
