from dotenv import load_dotenv
import os
import snowflake.connector

load_dotenv("lab/.env")

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

cur = conn.cursor()

cur.execute("""
INSERT INTO TRANSACTIONS
(transaction_id, merchant_name, amount, transaction_ts)
VALUES
('TEST001','ChatGPT Demo',999.99,CURRENT_TIMESTAMP())
""")

conn.commit()

cur.execute("SELECT COUNT(*) FROM TRANSACTIONS")
print(cur.fetchone())

cur.close()
conn.close()
