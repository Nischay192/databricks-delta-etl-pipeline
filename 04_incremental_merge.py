# Incremental Processing - MERGE Logic

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

# Simulated new batch data (Batch 2)
df_batch2 = spark.table("bronze_sales_orders")  # Replace with actual new batch in real scenario

# Apply Silver cleaning logic to Batch 2
df_batch2_clean = (
    df_batch2
    .filter(col("order_id").isNotNull())
    .filter(col("amount").isNotNull())
    .filter(col("amount") > 0)
    .withColumn("country", coalesce(col("country"), lit("UNKNOWN")))
)

# Load existing Silver table
delta_silver = DeltaTable.forName(spark, "silver_sales_orders")

# MERGE logic (Upsert)
delta_silver.alias("target").merge(
    df_batch2_clean.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("Incremental merge completed successfully.")