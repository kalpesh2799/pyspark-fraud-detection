from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# 1. INITIALIZE SPARK 4.0 SESSION
# We enable RocksDB for state management to handle millions of cards efficiently
spark = SparkSession.builder \
    .appName("RealTime_Fraud_Detection_kalpesh2799") \
    .config("spark.sql.streaming.stateStore.providerClass", 
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
    .getOrCreate()

# 2. DEFINE THE DATA SCHEMA
schema = StructType() \
    .add("txn_id", StringType()) \
    .add("card_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("city", StringType()) \
    .add("timestamp", TimestampType())

# 3. CONNECT TO KAFKA (Bronze Layer)
# Replace 'localhost:9092' with your actual Kafka broker if needed
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .load() \
    .select(F.from_json(F.col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# 4. REGISTER THE GRAND LOGIC (Silver Layer)
# We turn the live stream into a SQL view
raw_stream.createOrReplaceTempView("v_live_txns")

# 5. DEFINE FRAUD RULES USING SPARK SQL
# Rule: Flag high velocity (3+ txns/min) or high spend (> 50k)
fraud_alerts = spark.sql("""
    SELECT 
        window.start as alert_time,
        card_id,
        count(txn_id) as txn_count,
        sum(amount) as total_spent,
        CASE 
            WHEN sum(amount) > 50000 THEN 'CRITICAL: High Value'
            WHEN count(txn_id) > 3 THEN 'WARNING: High Velocity'
            ELSE 'CLEAN'
        END as risk_score
    FROM v_live_txns
    /* Watermark handles data arriving up to 10 minutes late */
    WATERMARK v_live_txns.timestamp DELAY 10 MINUTES
    GROUP BY window(timestamp, '1 minute'), card_id
    HAVING risk_score != 'CLEAN'
""")

# 6. OUTPUT THE ALERTS
# In production, change .format("console") to .format("delta") or another Kafka topic
query = fraud_alerts.writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime='2 seconds') \
    .start()

query.awaitTermination()
