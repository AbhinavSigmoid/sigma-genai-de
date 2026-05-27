# Data Pipeline Design Document

## What This Pipeline Does
This pipeline ingests transaction data, cleans it, enriches it with merchant details, and aggregates it into merchant performance metrics and daily summaries.

## Data Flow Diagram

```
+----------------+       +--------------------+       +-------------------+       +---------------------------+
|  Source Data   | --->  |  Bronze Layer      | --->  |  Silver Layer     | --->  |  Gold Layer                |
|  (TRANSACTIONS) |       |  (bronze_transactions) |       |  (silver_transactions) |       |  (gold_merchant_performance,  |
|                |       |                       |       |                     |       |  gold_daily_summary)        |
+----------------+       +--------------------+       +-------------------+       +---------------------------+
|                |       |                     |       |                     |       |                           |
|  (MERCHANTS)   | --->  |                     |       |                     |       |                           |
+----------------+       |                     |       |                     |       |                           |
```

## Key Design Decisions
- **Layered Approach**: The pipeline uses a bronze, silver, and gold layer to ensure data quality and transformation are modular and maintainable.
- **Enrichment**: Merchant details are joined with transaction data in the silver layer to provide enriched data for further analysis.
- **Aggregation**: The gold layer computes both merchant-specific and daily summary metrics, providing a comprehensive view of transaction performance.
- **Quality Flags**: Transactions are flagged for quality in the silver layer, allowing for easy filtering and analysis of clean data.

## Known Limitations
- **Data Freshness**: The pipeline does not handle real-time data ingestion; it processes batch data.
- **Error Handling**: The pipeline has minimal error handling, which could lead to data loss in case of failures.
- **Schema Changes**: The pipeline does not handle schema changes dynamically; it requires manual updates.
- **Data Volume**: The pipeline is not optimized for very large datasets, which could lead to performance issues.

## Dependencies
- **DuckDB**: The database engine used for storing and querying data.
- **MERCHANTS**: A list of merchant details used for enriching transaction data.
- **TRANSACTIONS_CLEAN and TRANSACTIONS_DIRTY**: Source data files containing clean and dirty transaction records.