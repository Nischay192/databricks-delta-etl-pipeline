# Bronze Layer - Raw Data Ingestion

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Read raw CSV data
df_raw = spark.read.csv(
    "/databricks-datasets/retail-org/sales_orders/sales_orders.csv",
    header=True,
    inferSchema=True
)

# Write to Bronze Delta table
df_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_sales_orders")

print("Bronze table created successfully.")
