import sys
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import from_json, col

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVER = "172.28.204.229:9092"
NET_TOPIC = "topic-net"
DISK_TOPIC = "topic-disk"
# ---------------------

def read_kafka_batch(spark, topic, schema):
    """Reads a Kafka topic as a batch and parses JSON."""
    return (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
    )

def write_single_csv(df, final_path):
    """Writes a Spark DataFrame to a single clean CSV file."""
    temp_dir = final_path + "_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)
    csv_files = [f for f in os.listdir(temp_dir) if f.endswith(".csv")]
    if csv_files:
        shutil.move(os.path.join(temp_dir, csv_files[0]), final_path)
    shutil.rmtree(temp_dir)

def main():
    spark = (
        SparkSession.builder
        .appName("Consumer2_NET_DISK_Raw")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("🚀 Consumer2 starting: Reading Kafka and saving raw data...")

    # --- Schemas ---
    net_schema = StructType([
        StructField("ts", StringType(), True),
        StructField("server_id", StringType(), True),
        StructField("net_in", FloatType(), True),
        StructField("net_out", FloatType(), True)
    ])
    disk_schema = StructType([
        StructField("ts", StringType(), True),
        StructField("server_id", StringType(), True),
        StructField("disk_io", FloatType(), True)
    ])

    # --- Read Kafka topics ---
    net_df = read_kafka_batch(spark, NET_TOPIC, net_schema)
    disk_df = read_kafka_batch(spark, DISK_TOPIC, disk_schema)

    # --- Save raw data ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    write_single_csv(net_df.dropDuplicates(["server_id", "ts"]).orderBy("server_id", "ts"),
                     os.path.join(script_dir, "net_data.csv"))
    write_single_csv(disk_df.dropDuplicates(["server_id", "ts"]).orderBy("server_id", "ts"),
                     os.path.join(script_dir, "disk_data.csv"))

    print(" Saved raw net and disk data.")
    spark.stop()

if __name__ == "__main__":
    main()