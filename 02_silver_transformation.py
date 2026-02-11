# Silver Layer - Data Cleaning & Transformation

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit

spark = SparkSession.builder.getOrCreate()

# Read Bronze table
df_bronze = spark.table("bronze_sales_orders")

# Apply cleaning logic
df_silver = (
    df_bronze
    .filter(col("order_id").isNotNull())
    .filter(col("amount").isNotNull())
    .filter(col("amount") > 0)
    .withColumn("country", coalesce(col("country"), lit("UNKNOWN")))
)

# Write to Silver Delta table
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_sales_orders")

print("Silver table created successfully.")
