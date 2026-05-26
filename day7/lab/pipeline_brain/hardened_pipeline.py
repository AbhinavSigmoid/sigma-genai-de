import logging
import shutil
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, broadcast, lit, sum, count, when, avg, min, max, mode
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType, DecimalType

logging.basicConfig(level=logging.INFO)

def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    try:
        schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("gateway_name", StringType(), True),
            StructField("amount", StringType(), True),
            StructField("transaction_date", StringType(), True),
        ])

        razorpay_df = spark.read.csv(f"{input_path}/razorpay_settlement_file.csv", schema=schema, header=True)
        payu_df = spark.read.csv(f"{input_path}/payu_settlement_file.csv", schema=schema, header=True)
        stripe_df = spark.read.json(f"{input_path}/stripe_settlement_file.json")

        razorpay_df = razorpay_df.withColumn("ingestion_timestamp", current_timestamp()) \
                                 .withColumn("source_file_name", lit("razorpay_settlement_file")) \
                                .withColumn("batch_id", lit(run_id)) \
                                .withColumn("load_date", lit(run_date))
        
        payu_df = payu_df.withColumn("ingestion_timestamp", current_timestamp()) \
                          .withColumn("source_file_name", lit("payu_settlement_file")) \
                          .withColumn("batch_id", lit(run_id)) \
                          .withColumn("load_date", lit(run_date))
        
        stripe_df = stripe_df.withColumn("ingestion_timestamp", current_timestamp()) \
                             .withColumn("source_file_name", lit("stripe_settlement_file")) \
                             .withColumn("batch_id", lit(run_id)) \
                             .withColumn("load_date", lit(run_date))

        partition_path = f"{output_path}/razorpay/load_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        razorpay_df.write.mode("overwrite").parquet(partition_path)
        logging.info(f"[Stage: Ingest Bronze] Razorpay: {razorpay_df.count():,} rows")

        partition_path = f"{output_path}/payu/load_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        payu_df.write.mode("overwrite").parquet(partition_path)
        logging.info(f"[Stage: Ingest Bronze] PayU: {payu_df.count():,} rows")

        partition_path = f"{output_path}/stripe/load_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        stripe_df.write.mode("overwrite").parquet(partition_path)
        logging.info(f"[Stage: Ingest Bronze] Stripe: {stripe_df.count():,} rows")

    except Exception as e:
        logging.error(f"[Stage: Ingest Bronze] Error: {e}")
        raise

def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    try:
        razorpay_df = spark.read.parquet(f"{bronze_path}/razorpay/load_date={run_date}")
        payu_df = spark.read.parquet(f"{bronze_path}/payu/load_date={run_date}")
        stripe_df = spark.read.parquet(f"{bronze_path}/stripe/load_date={run_date}")

        razorpay_df = razorpay_df.withColumn("amount", col("amount").cast(FloatType())) \
                                .withColumn("transaction_date", col("transaction_date").cast(DateType()))
        payu_df = payu_df.withColumn("amount", col("amount").cast(FloatType())) \
                          .withColumn("transaction_date", col("transaction_date").cast(DateType()))
        stripe_df = stripe_df.withColumn("amount", col("amount").cast(FloatType())) \
                             .withColumn("transaction_date", col("transaction_date").cast(DateType()))

        razorpay_df = razorpay_df.filter(col("transaction_id").isNotNull() & (col("amount") > 0))
        payu_df = payu_df.filter(col("transaction_id").isNotNull() & (col("amount") > 0))
        stripe_df = stripe_df.filter(col("transaction_id").isNotNull() & (col("amount") > 0))

        logging.info(f"[Stage: Transform Silver] Razorpay after filter: {razorpay_df.count():,} rows")
        logging.info(f"[Stage: Transform Silver] PayU after filter: {payu_df.count():,} rows")
        logging.info(f"[Stage: Transform Silver] Stripe after filter: {stripe_df.count():,} rows")

        razorpay_df = razorpay_df.dropDuplicates(["transaction_id", "gateway_name"], ["ingestion_timestamp"]).orderBy("ingestion_timestamp", ascending=False)
        payu_df = payu_df.dropDuplicates(["transaction_id", "gateway_name"], ["ingestion_timestamp"]).orderBy("ingestion_timestamp", ascending=False)
        stripe_df = stripe_df.dropDuplicates(["transaction_id", "gateway_name"], ["ingestion_timestamp"]).orderBy("ingestion_timestamp", ascending=False)

        logging.info(f"[Stage: Transform Silver] Razorpay after dedup: {razorpay_df.count():,} rows")
        logging.info(f"[Stage: Transform Silver] PayU after dedup: {payu_df.count():,} rows")
        logging.info(f"[Stage: Transform Silver] Stripe after dedup: {stripe_df.count():,} rows")

        merchants_df = spark.read.parquet(merchants_path)

        razorpay_df = razorpay_df.join(broadcast(merchants_df), ["transaction_id"], "left")
        payu_df = payu_df.join(broadcast(merchants_df), ["transaction_id"], "left")
        stripe_df = stripe_df.join(broadcast(merchants_df), ["transaction_id"], "left")

        razorpay_df = razorpay_df.withColumn("quality_flag", when(col("merchant_id").isNull(), "UNMATCHED").otherwise("CLEAN"))
        payu_df = payu_df.withColumn("quality_flag", when(col("merchant_id").isNull(), "UNMATCHED").otherwise("CLEAN"))
        stripe_df = stripe_df.withColumn("quality_flag", when(col("merchant_id").isNull(), "UNMATCHED").otherwise("CLEAN"))

        partition_path = f"{output_path}/razorpay/load_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        razorpay_df.write.mode("overwrite").parquet(partition_path)
        logging.info(f"[Stage: Transform Silver] Razorpay output: {razorpay_df.count():,} rows")

        partition_path = f"{output_path}/payu/load_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        payu_df.write.mode("overwrite").parquet(partition_path)
        logging.info(f"[Stage: Transform Silver] PayU output: {payu_df.count():,} rows")

        partition_path = f"{output_path}/stripe/load_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        stripe_df.write.mode("overwrite").parquet(partition_path)
        logging.info(f"[Stage: Transform Silver] Stripe output: {stripe_df.count():,} rows")

    except Exception as e:
        logging.error(f"[Stage: Transform Silver] Error: {e}")
        raise

def build_merchant_performance(spark, silver_path, output_path, run_date):
    try:
        silver_df = spark.read.parquet(silver_path).where(col("status") == "COMPLETED").filter(col("date") == run_date)  # Partition pruning

        merchant_performance_df = silver_df.groupBy("merchant_id", "merchant_name", "category", "city", "date") \
           .agg(
                sum("amount").alias("total_revenue"),
                count("*").alias("txn_count"),
                (count(when(col("status") == "FAILED", 1)) / count("*") * 100).alias("failure_rate_pct")
            )

        partition_path = f"{output_path}/load_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        merchant_performance_df.repartition("date").write.mode("overwrite").parquet(partition_path)
        logging.info(f"[Stage: Build Merchant Performance] Output: {merchant_performance_df.count():,} rows")

    except Exception as e:
        logging.error(f"[Stage: Build Merchant Performance] Error: {e}")
        raise

def build_customer_ltv(spark, silver_path, output_path):
    try:
        silver_df = spark.read.parquet(silver_path).where(col("status") == "COMPLETED")

        customer_ltv_df = silver_df.groupBy("customer_id") \
           .agg(
                sum("amount").alias("total_spent"),
                count("*").alias("total_txns"),
                avg("amount").alias("avg_txn_value"),
                min("transaction_date").alias("first_txn_date"),
                max("transaction_date").alias("last_txn_date"),
                mode("payment_method").alias("preferred_payment_method")
           )

        customer_ltv_df.write.mode("overwrite").parquet(output_path)
        logging.info(f"[Stage: Build Customer LTV] Output: {customer_ltv_df.count():,} rows")

    except Exception as e:
        logging.error(f"[Stage: Build Customer LTV] Error: {e}")
        raise

def build_daily_summary(spark, silver_path, output_path, run_date):
    try:
        silver_df = spark.read.parquet(silver_path).where(col("status") == "COMPLETED").filter(col("date") == run_date)  # Partition pruning

        daily_summary_df = silver_df.groupBy("date") \
            .agg(
                sum("amount").alias("total_revenue"),
                count("*").alias("total_txns"),
                count(col("customer_id").alias("unique_customers")),
                count(col("merchant_id").alias("unique_merchants")),
                (count(when(col("status") == "FAILED", 1)) / count("*") * 100).alias("failure_rate_pct")
            )

        partition_path = f"{output_path}/load_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        daily_summary_df.repartition("date").write.mode("overwrite").parquet(partition_path)
        logging.info(f"[Stage: Build Daily Summary] Output: {daily_summary_df.count():,} rows")

    except Exception as e:
        logging.error(f"[Stage: Build Daily Summary] Error: {e}")
        raise

def run_gold(spark, silver_path, gold_output_dir, run_date):
    try:
        merchant_performance_output_path = f"{gold_output_dir}/merchant_performance"
        customer_ltv_output_path = f"{gold_output_dir}/customer_ltv"
        daily_summary_output_path = f"{gold_output_dir}/daily_summary"

        build_merchant_performance(spark, silver_path, merchant_performance_output_path, run_date)
        build_customer_ltv(spark, silver_path, customer_ltv_output_path)
        build_daily_summary(spark, silver_path, daily_summary_output_path, run_date)

        run_metadata = {
            "pipeline_name": "Sigma DataTech Transaction Analytics Pipeline",
            "run_date": run_date,
            "run_id": "12345",
            "run_status": "SUCCESS",
            "started_at": datetime.now().isoformat(),
            "completed_at": datetime.now().isoformat(),
            "merchant_performance_output_path": merchant_performance_output_path,
            "customer_ltv_output_path": customer_ltv_output_path,
            "daily_summary_output_path": daily_summary_output_path
        }

        spark.sparkContext.parallelize([run_metadata]).write.json(f"{gold_output_dir}/run_metadata")

    except Exception as e:
        logging.error(f"[Stage: Run Gold] Error: {e}")
        run_metadata = {
            "pipeline_name": "Sigma DataTech Transaction Analytics Pipeline",
            "run_date": run_date,
            "run_id": "12345",
            "run_status": "FAILED",
            "error_message": str(e),
            "started_at": datetime.now().isoformat(),
            "completed_at": datetime.now().isoformat()
        }
        spark.sparkContext.parallelize([run_metadata]).write.json(f"{gold_output_dir}/run_metadata")
        raise

def main(spark, input_path, bronze_output_path, merchants_path, silver_output_path, run_date, run_id):
    try:
        ingest_bronze(spark, input_path, bronze_output_path, run_date, run_id)
        transform_silver(spark, bronze_output_path, merchants_path, silver_output_path, run_date)
    except Exception as e:
        logging.error(f"[Main] Error: {e}")
        raise

if __name__ == "__main__":
    spark = SparkSession.builder \
      .appName("Reconciliation Pipeline") \
        .getOrCreate()

    input_path = "s3://your-bucket/input/"
    bronze_output_path = "s3://your-bucket/bronze/"
    merchants_path = "s3://your-bucket/merchants/"
    silver_output_path = "s3://your-bucket/silver/"
    gold_output_dir = "s3://your-bucket/gold/"
    run_date = "2023-10-01"
    run_id = "12345"

    main(spark, input_path, bronze_output_path, merchants_path, silver_output_path, run_date, run_id)
