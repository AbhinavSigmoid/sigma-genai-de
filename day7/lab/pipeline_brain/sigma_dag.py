from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import os

default_args = {
    'owner': 'data-engineering',
   'retries': 2,
   'retry_delay': timedelta(minutes=5),
    'email_on_failure': True
}

def log_failure(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    exception = context['exception']
    logging.error(f"Dag: {dag_id}, Task: {task_id}, Execution Date: {execution_date}, Error: {exception}")

def sla_miss_callback(context):
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    logging.error(f"Dag: {dag_id}, Execution Date: {execution_date}, SLA Miss")

def extract_bronze(**context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    logging.info(f"{dag_id}, {task_id}, {execution_date}, Starting Bronze Extraction")
    
    try:
        # Read CSV files
        transactions_df = pd.read_csv('transactions.csv')
        merchants_df = pd.read_csv('merchants.csv')
        
        # Add metadata columns
        transactions_df['ingestion_timestamp'] = execution_date
        transactions_df['source_file'] = 'transactions.csv'
        transactions_df['pipeline_run_id'] = dag_id
        
        # Write to Parquet
        transactions_df.write_parquet('bronze/transactions.parquet', partition_cols=['ingestion_timestamp'])
        merchants_df.write_parquet('bronze/merchants.parquet', partition_cols=['ingestion_timestamp'])
        
        logging.info(f"{dag_id}, {task_id}, {execution_date}, Bronze Extraction Completed")
    except Exception as e:
        logging.error(f"{dag_id}, {task_id}, {execution_date}, Bronze Extraction Failed: {e}")
        raise e

def transform_silver(**context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    logging.info(f"{dag_id}, {task_id}, {execution_date}, Starting Silver Transformation")
    
    try:
        # Read Bronze Parquet
        transactions_df = pd.read_parquet('bronze/transactions.parquet', filters=[('ingestion_timestamp', '=', execution_date.strftime('%Y-%m-%d'))])
        merchants_df = pd.read_parquet('bronze/merchants.parquet', filters=[('ingestion_timestamp', '=', execution_date.strftime('%Y-%m-%d'))])
        
        # Cast columns
        transactions_df['amount'] = transactions_df['amount'].astype(float)
        transactions_df['transaction_date'] = pd.to_datetime(transactions_df['transaction_date'])
        
        # Filter and deduplicate
        transactions_df = transactions_df[transactions_df['transaction_id'].notnull() & (transactions_df['amount'] >= 0)]
        transactions_df = transactions_df.sort_values('ingestion_timestamp').drop_duplicates(subset='transaction_id', keep='last')
        
        # Join with merchants
        transactions_df = transactions_df.merge(merchants_df[['merchant_id','merchant_name', 'category', 'city']], on='merchant_id', how='left')
        
        # Add quality flag
        transactions_df['quality_flag'] = transactions_df['merchant_id'].apply(lambda x: 'UNMATCHED' if pd.isnull(x) else 'MATCHED')
        
        # Write to Parquet
        transactions_df.write_parquet('silver/transactions.parquet', partition_cols=['ingestion_timestamp'])
        
        logging.info(f"{dag_id}, {task_id}, {execution_date}, Silver Transformation Completed")
    except Exception as e:
        logging.error(f"{dag_id}, {task_id}, {execution_date}, Silver Transformation Failed: {e}")
        raise e

def build_gold(**context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    logging.info(f"{dag_id}, {task_id}, {execution_date}, Starting Gold Build")
    
    try:
        # Read Silver Parquet
        transactions_df = pd.read_parquet('silver/transactions.parquet', filters=[('ingestion_timestamp', '=', execution_date.strftime('%Y-%m-%d'))])
        
        # Gold Table 1: merchant_performance
        merchant_performance_df = transactions_df.groupby(['merchant_id','merchant_name', 'category', 'city', 'transaction_date']).agg(
            total_revenue=pd.NamedAgg(column='amount', aggfunc=lambda x: (x[transactions_df['status'] == 'COMPLETED']).sum()),
            txn_count=('transaction_id', 'count'),
            failure_rate_pct=pd.NamedAgg(column='status', aggfunc=lambda x: ((x == 'FAILED').sum() / len(x)) * 100)
        ).reset_index()
        
        merchant_performance_df.write_parquet('gold/merchant_performance.parquet', partition_cols=['transaction_date'])
        
        # Gold Table 2: customer_ltv
        customer_ltv_df = transactions_df.groupby('customer_id').agg(
            total_spent=('amount', 'sum'),
            total_txns=('transaction_id', 'count'),
            avg_txn_value=pd.NamedAgg(column='amount', aggfunc='mean'),
            first_txn_date=('transaction_date','min'),
            last_txn_date=('transaction_date','max'),
            preferred_payment_method=('payment_method', lambda x: x.mode()[0])
        ).reset_index()
        
        customer_ltv_df.write_parquet('gold/customer_ltv.parquet', partition_cols=['transaction_date'])
        
        # Gold Table 3: daily_summary
        daily_summary_df = transactions_df.groupby('transaction_date').agg(
            total_revenue=pd.NamedAgg(column='amount', aggfunc=lambda x: (x[transactions_df['status'] == 'COMPLETED']).sum()),
            total_txns=('transaction_id', 'count'),
            unique_customers=('customer_id', 'nunique'),
            unique_merchants=('merchant_id', 'nunique'),
            failure_rate_pct=pd.NamedAgg(column='status', aggfunc=lambda x: ((x == 'FAILED').sum() / len(x)) * 100)
        ).reset_index()
        
        daily_summary_df.write_parquet('gold/daily_summary.parquet', partition_cols=['transaction_date'])
        
        logging.info(f"{dag_id}, {task_id}, {execution_date}, Gold Build Completed")
    except Exception as e:
        logging.error(f"{dag_id}, {task_id}, {execution_date}, Gold Build Failed: {e}")
        raise e

with DAG(
    dag_id='sigma_transaction_pipeline',
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    on_failure_callback=log_failure,
    sla_miss_callback=sla_miss_callback,
    tags=["sigma", "transactions", "daily"],
    description="Daily Bronze->Silver->Gold pipeline for Sigma DataTech transactions"
) as dag:

    start = DummyOperator(task_id='start')

    extract_bronze_task = PythonOperator(
        task_id='extract_bronze',
        python_callable=extract_bronze,
        on_failure_callback=log_failure
    )

    transform_silver_task = PythonOperator(
        task_id='transform_silver',
        python_callable=transform_silver,
        on_failure_callback=log_failure
    )

    build_gold_task = PythonOperator(
        task_id='build_gold',
        python_callable=build_gold,
        on_failure_callback=log_failure
    )

    end = DummyOperator(task_id='end')

    start >> extract_bronze_task >> transform_silver_task >> build_gold_task >> end
