# Pipeline Overview

This pipeline processes transaction data, transforms it into a cleaned and enriched format, and computes merchant performance and daily summary metrics. It runs to ensure data is up-to-date for reporting and analytics. If it stops, downstream reports and dashboards will become stale.

## Pipeline Steps

1. Connect to the DuckDB database using `get_connection`.
2. Set up required tables using `setup_tables`.
3. Load merchant data into the `merchants` table using `load_merchants`.
4. Load raw transactions into the `bronze_transactions` table using `load_bronze`.
5. Transform raw transactions into cleaned and enriched data, loading into the `silver_transactions` table using `transform_bronze_to_silver` and `load_silver`.
6. Compute merchant performance metrics and load into the `gold_merchant_performance` table using `compute_merchant_performance` and `load_gold`.
7. Compute daily summary metrics and load into the `gold_daily_summary` table using `compute_daily_summary` and `load_gold`.

## Schedule / Trigger

This pipeline runs every night at 2 AM UTC via a cron job.

## Failure Modes

1. **Database Connection Failure**
   - **Root Cause:** DuckDB service is down.
   - **Symptom:** `get_connection` fails.
2. **Table Creation Failure**
   - **Root Cause:** Syntax error in SQL.
   - **Symptom:** `setup_tables` fails.
3. **Merchant Data Load Failure**
   - **Root Cause:** Corrupt merchant data.
   - **Symptom:** `load_merchants` fails.
4. **Bronze Table Load Failure**
   - **Root Cause:** Invalid transaction data.
   - **Symptom:** `load_bronze` fails.
5. **Silver Table Transformation Failure**
   - **Root Cause:** Missing merchant data for transactions.
   - **Symptom:** `transform_bronze_to_silver` fails.

## Recovery Actions

1. **Database Connection Failure**
   - Check DuckDB service status.
   - Restart the DuckDB service if down.
   - Retry the pipeline.
2. **Table Creation Failure**
   - Review and correct the SQL in `setup_tables`.
   - Rerun the pipeline.
3. **Merchant Data Load Failure**
   - Validate and correct the merchant data.
   - Rerun the pipeline.
4. **Bronze Table Load Failure**
   - Validate and correct the transaction data.
   - Rerun the pipeline.
5. **Silver Table Transformation Failure**
   - Ensure all merchants in transactions are present in the `merchants` table.
   - Rerun the pipeline.

## Known Bugs

- Hardcoded AWS credentials in the code.
- Lack of null handling in `transform_bronze_to_silver`.

## Escalation Contacts

1. **On-call DE:** Priya Nair (priya.nair@sigmadatatech.in, +91-98400-11111)
2. **Tech Lead:** Arjun Mehta (arjun.mehta@sigmadatatech.in)
3. **Platform Manager:** Kavya Reddy (kavya.reddy@sigmadatatech.in)

## Data Quality Checks

- Verify the count of records in `silver_transactions` matches the input transactions.
- Check `gold_merchant_performance` for expected merchant IDs and revenue values.
- Ensure `gold_daily_summary` has today's date with non-zero total revenue and transactions.