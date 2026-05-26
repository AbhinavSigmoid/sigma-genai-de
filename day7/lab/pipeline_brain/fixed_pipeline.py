# ========================= fixed_pipeline.py =========================

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    broadcast,
    lit,
    sum,
    count,
    when,
    avg,
    min,
    max
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    DateType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ingest_bronze(spark, input_path, output_path, run_date, run_id):

    try:

        logger.info("Starting Bronze ingestion")

        schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("merchant_id", StringType(), True),
            StructField("gateway_name", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("status", StringType(), True),
            StructField("amount", StringType(), True),
            StructField("transaction_date", StringType(), True),
        ])

        razorpay_df = spark.read.csv(
            f"{input_path}/razorpay_settlement_file.csv",
            schema=schema,
            header=True
        )

        required_cols = ["transaction_id", "amount"]

        for c in required_cols:
            if c not in razorpay_df.columns:
                raise Exception(f"Missing required column: {c}")

        razorpay_df = (
            razorpay_df
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file_name", lit("razorpay_settlement_file"))
            .withColumn("batch_id", lit(run_id))
            .withColumn("load_date", lit(run_date))
        )

        logger.info(f"Bronze row count: {razorpay_df.count()}")

        razorpay_df.write.mode("overwrite").parquet(
            f"{output_path}/razorpay/load_date={run_date}"
        )

        logger.info("Bronze ingestion completed")

    except Exception as e:
        logger.error(f"Bronze ingestion failed: {e}")
        raise


def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):

    try:

        logger.info("Starting Silver transformation")

        bronze_df = spark.read.parquet(
            f"{bronze_path}/razorpay/load_date={run_date}"
        )

        required_cols = ["transaction_id", "amount"]

        for c in required_cols:
            if c not in bronze_df.columns:
                raise Exception(f"Missing required column: {c}")

        bronze_df = (
            bronze_df
            .withColumn("amount", col("amount").cast(FloatType()))
            .withColumn("transaction_date", col("transaction_date").cast(DateType()))
        )

        bronze_df = bronze_df.filter(
            col("transaction_id").isNotNull() &
            (col("amount") > 0)
        )

        logger.info(f"After filtering rows: {bronze_df.count()}")

        bronze_df = bronze_df.dropDuplicates(
            ["transaction_id", "gateway_name"]
        )

        logger.info(f"After dedup rows: {bronze_df.count()}")

        merchants_df = spark.read.parquet(merchants_path)

        silver_df = bronze_df.join(
            broadcast(merchants_df),
            ["merchant_id"],
            "left"
        )

        silver_df = silver_df.withColumn(
            "quality_flag",
            lit("CLEAN")
        )

        logger.info(f"Silver row count: {silver_df.count()}")

        silver_df.write.mode("overwrite").parquet(
            f"{output_path}/razorpay/load_date={run_date}"
        )

        logger.info("Silver transformation completed")

    except Exception as e:
        logger.error(f"Silver transformation failed: {e}")
        raise


def build_gold(spark, silver_path, output_path):

    try:

        logger.info("Starting Gold aggregation")

        silver_df = spark.read.parquet(silver_path).filter(
            col("status") == "COMPLETED"
        )

        gold_df = silver_df.groupBy("merchant_id").agg(
            sum("amount").alias("total_revenue"),
            count("*").alias("txn_count"),
            avg("amount").alias("avg_txn_value")
        )

        logger.info(f"Gold row count: {gold_df.count()}")

        gold_df.write.mode("overwrite").parquet(output_path)

        logger.info("Gold layer completed")

    except Exception as e:
        logger.error(f"Gold layer failed: {e}")
        raise


def main():

    spark = SparkSession.builder \
        .appName("ReconciliationPipeline") \
        .getOrCreate()

    input_path = os.getenv("INPUT_PATH")
    bronze_output_path = os.getenv("BRONZE_PATH")
    silver_output_path = os.getenv("SILVER_PATH")
    gold_output_path = os.getenv("GOLD_PATH")
    merchants_path = os.getenv("MERCHANTS_PATH")

    if not all([
        input_path,
        bronze_output_path,
        silver_output_path,
        gold_output_path,
        merchants_path
    ]):
        raise Exception("Missing required environment variables")

    run_date = "2023-10-01"
    run_id = "12345"

    try:

        ingest_bronze(
            spark,
            input_path,
            bronze_output_path,
            run_date,
            run_id
        )

        transform_silver(
            spark,
            bronze_output_path,
            merchants_path,
            silver_output_path,
            run_date
        )

        build_gold(
            spark,
            silver_output_path,
            gold_output_path
        )

        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise


if __name__ == "__main__":
    main()