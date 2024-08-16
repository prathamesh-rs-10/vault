import sys
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("Data Transformation").getOrCreate()

# Read data from Kafka
df = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "profit_loss_data") \
    .load()

# Apply transformations
df_transformed = df.select(
    "Sales",
    "Expenses",
    "Operating Profit",
    "OPM %",
    "Other Income",
    "Interest",
    "Depreciation",
    "Profit before tax",
    "Tax %",
    "Net Profit",
    "EPS in Rs",
    "Dividend Payout %"
) \
    .filter(df["Sales"] > 0) \
    .groupBy("Operating Profit") \
    .agg(
        avg("Sales").alias("Average Sales"),
        sum("Expenses").alias("Total Expenses"),
        max("Operating Profit").alias("Max Operating Profit"),
        min("OPM %").alias("Min OPM"),
        avg("Other Income").alias("Average Other Income"),
        sum("Interest").alias("Total Interest"),
        max("Depreciation").alias("Max Depreciation"),
        avg("Profit before tax").alias("Average Profit before tax"),
        sum("Net Profit").alias("Total Net Profit"),
        avg("EPS in Rs").alias("Average EPS"),
        max("Dividend Payout %").alias("Max Dividend Payout")
    )

# Display the transformed data
df_transformed.show()

# Stop the Spark session
spark.stop()