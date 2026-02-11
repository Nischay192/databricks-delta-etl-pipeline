# Gold Layer - Business Aggregations

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, avg, round as spark_round

spark = SparkSession.builder.getOrCreate()

# Read Silver table
df_silver = spark.table("silver_sales_orders")

# Aggregate by country
df_gold = (
    df_silver
    .groupBy("country")
    .agg(
        count("order_id").alias("total_orders"),
        spark_round(sum("amount"), 2).alias("total_sales"),
        spark_round(avg("amount"), 2).alias("avg_order_value")
    )
)

# Write Gold table
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_sales_by_country")

print("Gold table created successfully.")