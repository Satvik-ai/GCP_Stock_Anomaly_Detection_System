from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, TimestampType
from google.cloud import pubsub_v1

# Initialize Spark session with the necessary configuration
spark = SparkSession.builder \
    .appName("StockAnomalyDetection") \
    .config("spark.sql.streaming.statefulOperator.allowMultiple", "true") \
    .getOrCreate()

KAFKA_TOPIC = "input-topic"
KAFKA_BOOTSTRAP_SERVERS = "34.28.179.159:9092"
PROJECT_ID = "eminent-crane-448810-s3"
TOPIC_NAME = "a2_anomaly_topic"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

# Define schema
schema = StructType() \
    .add("stock_id", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("close_price", DoubleType()) \
    .add("volume", LongType())

# Kafka stream
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON and timestamps
df = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp("timestamp")) \
    .withWatermark("timestamp", "10 minutes")  # Single watermark that works for both analyses

# ===== Process A1: Price deviation anomalies =====
# Group by stock_id and window to get current and previous prices
price_window = df \
    .groupBy(
        "stock_id",
        window("timestamp", "1 minute")
    ) \
    .agg(
        last("close_price").alias("current_price"),
        last("timestamp").alias("current_timestamp"),
        first("close_price").alias("prev_price"),
        first("timestamp").alias("prev_timestamp"),
        last("volume").alias("volume")  # Keep volume for later use
    ) \
    .filter(col("current_price").isNotNull() & col("prev_price").isNotNull() & 
            (col("current_timestamp") > col("prev_timestamp")))

# Calculate price deviation
price_deviation = price_window \
    .withColumn(
        "price_diff_pct",
        abs(col("current_price") - col("prev_price")) / col("prev_price") * 100
    )

# Filter for A1 anomalies
anomaly_a1 = price_deviation \
    .filter(col("price_diff_pct") > 0.5) \
    .select(
        "stock_id",
        col("current_timestamp").alias("timestamp"),
        col("current_price").alias("close_price"),
        "volume",
        lit("A1").alias("anomaly_type")
    )

# ===== Process A2: Volume spike anomalies =====
volume_avg = df \
    .groupBy(
        "stock_id",
        window("timestamp", "10 minutes", "1 minute")
    ) \
    .agg(
        avg("volume").alias("avg_volume"),
        last("close_price").alias("close_price"),
        last("timestamp").alias("timestamp"),
        last("volume").alias("volume")
    )

# Calculate volume deviation
volume_with_avg = volume_avg \
    .withColumn(
        "volume_diff_pct",
        (col("volume") - col("avg_volume")) / col("avg_volume") * 100
    )

# Filter for A2 anomalies
anomaly_a2 = volume_with_avg \
    .filter(col("volume_diff_pct") > 2) \
    .select(
        "stock_id",
        "timestamp",
        "close_price",
        "volume",
        lit("A2").alias("anomaly_type")
    )

# Combine both anomalies
anomalies = anomaly_a1.unionByName(anomaly_a2)

# ===== Console Sink (for monitoring) =====
console_query = anomalies.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# ===== Pub/Sub Sink for A2 anomalies =====
def publish_to_pubsub(df, epoch_id):
    try:
        a2_df = df.filter(df.anomaly_type == "A2").toPandas()
        for _, row in a2_df.iterrows():
            message = f"Traded Volume more than 2% of its average: {row.stock_id} at {row.timestamp}"
            publisher.publish(topic_path, message.encode("utf-8"))
    except Exception as e:
        print(f"Error publishing to PubSub: {e}")

pubsub_query = anomalies.writeStream \
    .outputMode("append") \
    .foreachBatch(publish_to_pubsub) \
    .start()

# Wait for termination of either query
spark.streams.awaitAnyTermination()