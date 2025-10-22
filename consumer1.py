import sys
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import from_json, col, round as spark_round

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVER = "172.28.204.229:9092"
CPU_TOPIC = "topic-cpu"
MEM_TOPIC = "topic-mem"
# ----------------------

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
    spark = SparkSession.builder.appName("Consumer1_CPU_MEM_Raw").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("🚀 Consumer1 starting: Reading Kafka and saving raw data...")

    # --- Schemas ---
    cpu_schema = StructType([
        StructField("ts", StringType(), True),
        StructField("server_id", StringType(), True),
        StructField("cpu_pct", FloatType(), True)
    ])
    mem_schema = StructType([
        StructField("ts", StringType(), True),
        StructField("server_id", StringType(), True),
        StructField("mem_pct", FloatType(), True)
    ])

    # --- Read Kafka topics ---
    cpu_df = read_kafka_batch(spark, CPU_TOPIC, cpu_schema)
    mem_df = read_kafka_batch(spark, MEM_TOPIC, mem_schema)

    # --- Round numeric columns to 2 decimal places ---
    cpu_df = cpu_df.withColumn("cpu_pct", spark_round(col("cpu_pct"), 2))
    mem_df = mem_df.withColumn("mem_pct", spark_round(col("mem_pct"), 2))

    # --- Save raw CPU/MEM data ---
    script_dir = os.path.dirname(os.path.abspath(file))
    cpu_path = os.path.join(script_dir, "cpu_data.csv")
    mem_path = os.path.join(script_dir, "mem_data.csv")

    write_single_csv(cpu_df.dropDuplicates(["server_id", "ts"]).orderBy("server_id", "ts"), cpu_path)
    write_single_csv(mem_df.dropDuplicates(["server_id", "ts"]).orderBy("server_id", "ts"), mem_path)

    print(f"✅ Saved raw CPU data to {cpu_path}")
    print(f"✅ Saved raw MEM data to {mem_path}")

    spark.stop()

if name == "main":
    main()