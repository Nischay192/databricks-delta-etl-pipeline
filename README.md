# Databricks Delta Lake ETL Pipeline

## Overview
This project implements an end-to-end ETL pipeline using Databricks and Delta Lake following the Medallion Architecture (Bronze → Silver → Gold).

The pipeline demonstrates incremental data ingestion, data quality enforcement, ACID-compliant Delta tables, and analytics-ready aggregations.

---

## Architecture

Bronze Layer
- Raw data ingestion
- Stored as Delta tables
- Schema inference and validation

Silver Layer
- Data cleaning and validation
- Null handling
- Deduplication
- Computed fields (total_amount)

Gold Layer
- Business-level aggregations
- Country-level sales metrics
- Analytics-ready outputs

---

## Key Features

- Apache Spark (PySpark)
- Delta Lake with ACID transactions
- Incremental upserts using MERGE
- Idempotent pipeline design
- Bronze-Silver-Gold architecture
- Data quality enforcement
- Aggregation and reporting

---

## Incremental Processing

Simulated new batch data and implemented MERGE logic to:

- Update existing records
- Insert new records
- Ensure safe reprocessing without duplication

---

## Technologies Used

- Databricks
- Apache Spark
- Delta Lake
- SQL
- Python

---

## Outcome

Built a production-style ETL pipeline demonstrating data engineering best practices suitable for real-world analytics environments.
