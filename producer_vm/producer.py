from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
import os
from kafka import KafkaProducer
import json

# 🔧 CONFIGURATION
KAFKA_TOPIC = "input-topic"
KAFKA_BOOTSTRAP_SERVERS = "34.28.179.159:9092"
CSV_ROOT_FOLDER = "./NSE_Stocks_Data"  # Root folder with nested CSV files

# 🚀 Create Spark Session
spark = SparkSession.builder \
    .appName("RecursiveCSVToKafkaStreamer") \
    .getOrCreate()

producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, value_serializer=lambda v: json.dumps(v).encode('utf-8'))


# 🔁 Recursively find all .csv files
for root, dirs, files in os.walk(CSV_ROOT_FOLDER):
    for file in files:
        if file.endswith(".csv"):
            full_path = os.path.join(root, file)
            print(f"\n📁 Processing: {full_path}")

            # Derive stock_id from filename (before first '')
            stock_id = file.split("__")[0]

            # Load CSV
            df = spark.read.option("header", True).csv(full_path)
            print(f"🔍 Raw rows in {file}: {df.count()}")

            # 🧹 Preprocess: add stock_id, cast types
            df = df.select(
                lit(stock_id).alias("stock_id"),
                to_timestamp("timestamp").alias("timestamp"),
                col("close").cast("double").alias("close_price"),
                col("volume").cast("long")
            )

            # Filter bad rows
            df = df.filter("stock_id IS NOT NULL AND timestamp IS NOT NULL AND close_price IS NOT NULL AND volume IS NOT NULL")
            
            # Sort the data
            df = df.orderBy(col("timestamp"))

            print(f"✅ Cleaned rows in {file}: {df.count()}")

            # Function to send a row to Kafka
            def send_to_kafka(row, producer, topic):
                # Convert row to dictionary
                row_dict = {
                    "stock_id": row["stock_id"],
                    "timestamp": str(row["timestamp"]),
                    "close_price": row["close_price"],
                    "volume": row["volume"]
                }
                
                # Send to Kafka
                producer.send(topic, row_dict)
                producer.flush()
                print("✅ Sent:", row)

            rows = df.collect()
            for row in rows:
                send_to_kafka(row, producer, KAFKA_TOPIC)
                
        time.sleep(10)  # Simulate 1-minute interval with 10 seconds