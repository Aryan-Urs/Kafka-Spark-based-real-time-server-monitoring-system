import sys
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, StringType, TimestampType, StructType, StructField
from pyspark.sql.functions import (
    col, avg, max, round, when,
    unix_timestamp, from_unixtime, date_format,
    ceil, floor, explode, sequence, lit
)

# --- Configuration ---
THRESHOLDS_FILE = "thresholds.txt"
WINDOW_SIZE = 30  # seconds
SLIDE = 10        # seconds
START_OFFSET = 0  # seconds
# ---------------------

def parse_thresholds(file_path):
    thresholds = {}
    try:
        with open(file_path, "r") as f:
            for line in f:
                if ":" in line:
                    key, value = line.split(":", 1)
                    thresholds[key.strip()] = float(value.strip())
        print(f"✅ Loaded thresholds: {thresholds}")
        return thresholds
    except Exception as e:
        print(f" Error reading thresholds file: {e}")
        sys.exit(1)

def write_single_csv(df, final_path):
    """Writes a Spark DataFrame to a single clean CSV file."""
    temp_dir = final_path + "_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)
    csv_files = [f for f in os.listdir(temp_dir) if f.endswith(".csv")]
    if csv_files:
        shutil.move(os.path.join(temp_dir, csv_files[0]), final_path)
    shutil.rmtree(temp_dir)

def main():
    thresholds = parse_thresholds(THRESHOLDS_FILE)
    cpu_thresh = thresholds.get("cpu_threshold", 99999.0)
    mem_thresh = thresholds.get("mem_threshold", 99999.0)

    spark = SparkSession.builder.appName("SparkJob1_CPU_MEM_Analysis").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(" SparkJob1 starting: Computing sliding windows and alerts...")

    # --- Schema for reading CSVs ---
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

    script_dir = os.path.dirname(os.path.abspath(__file__))
    cpu_path = os.path.join(script_dir, "cpu_data.csv")
    mem_path = os.path.join(script_dir, "mem_data.csv")

    # --- Read saved raw CSV data ---
    cpu_df = spark.read.option("header", "true").schema(cpu_schema).csv(cpu_path)
    mem_df = spark.read.option("header", "true").schema(mem_schema).csv(mem_path)

    # --- Convert timestamps ---
    cpu_df = cpu_df.withColumn("ts", col("ts").cast(TimestampType()))
    mem_df = mem_df.withColumn("ts", col("ts").cast(TimestampType()))
    cpu_df = cpu_df.withColumn("ts", from_unixtime(floor(unix_timestamp("ts"))).cast(TimestampType()))
    mem_df = mem_df.withColumn("ts", from_unixtime(floor(unix_timestamp("ts"))).cast(TimestampType()))

    cpu_df = cpu_df.dropDuplicates(["server_id", "ts"])
    mem_df = mem_df.dropDuplicates(["server_id", "ts"])

    # --- Join CPU and MEM data ---
    combined_df = cpu_df.join(mem_df, on=["server_id", "ts"], how="inner") \
        .select("server_id", "ts", "cpu_pct", "mem_pct")
    combined_df = combined_df.withColumn("ts_sec", unix_timestamp("ts"))

    # --- Align first window start ---
    min_ts = combined_df.agg({"ts_sec": "min"}).collect()[0][0]
    first_window_start = (min_ts // SLIDE) * SLIDE

    # --- Sliding window logic ---
    combined_df = combined_df.withColumn(
        "k_min", ceil((col("ts_sec") - (WINDOW_SIZE - 1) - START_OFFSET) / SLIDE)
    ).withColumn(
        "k_max", floor((col("ts_sec") - START_OFFSET) / SLIDE)
    )
    combined_df = combined_df.withColumn(
        "k_min",
        when(col("k_min") * SLIDE + START_OFFSET < first_window_start,
             (first_window_start - START_OFFSET) // SLIDE
            ).otherwise(col("k_min"))
    )
    combined_df = combined_df.withColumn("k", sequence(col("k_min"), col("k_max")))
    combined_df = combined_df.withColumn("k", explode(col("k")))
    combined_df = combined_df.withColumn(
        "window_start", from_unixtime((col("k") * SLIDE) + START_OFFSET).cast(TimestampType())
    ).withColumn(
        "window_end", from_unixtime((col("k") * SLIDE) + START_OFFSET + WINDOW_SIZE).cast(TimestampType())
    )
    combined_df = combined_df.filter(
        (unix_timestamp(col("window_start")) <= col("ts_sec")) &
        (col("ts_sec") < unix_timestamp(col("window_end")))
    )

    # --- Aggregation ---
    agg_df = combined_df.groupBy("server_id", "window_start", "window_end").agg(
        round(avg("cpu_pct"), 2).alias("avg_cpu"),
        round(avg("mem_pct"), 2).alias("avg_mem"),
        max("cpu_pct").alias("max_cpu"),
        max("mem_pct").alias("max_mem")
    )

    # --- Alert logic ---
    final_df = agg_df.select(
        "server_id",
        date_format(col("window_start"), "HH:mm:ss").alias("window_start"),
        date_format(col("window_end"), "HH:mm:ss").alias("window_end"),
        "avg_cpu", "avg_mem", "max_cpu", "max_mem",
        when(
            (col("avg_cpu") > float(cpu_thresh)) & (col("avg_mem") > float(mem_thresh)),
            "High CPU + Memory stress"
        ).when(
            (col("avg_cpu") > float(cpu_thresh)) & (col("avg_mem") <= float(mem_thresh)),
            "CPU spike suspected"
        ).when(
            (col("avg_mem") > float(mem_thresh)) & (col("avg_cpu") <= float(cpu_thresh)),
            "Memory saturation suspected"
        ).otherwise(lit("")).alias("alert")
    )

    final_df = final_df.dropDuplicates(["server_id", "window_start", "window_end"])
    final_df = final_df.orderBy("server_id", "window_start")

    # --- Save output ---
    output_path = os.path.join(script_dir, "team_87_CPU_MEM.csv")
    write_single_csv(final_df, output_path)

    # --- Summary ---
    total_rows = final_df.count()
    alerts = final_df.filter(col("alert") != "").count()
    print(f" Job complete. Output file: {output_path}")
    print(f" Total rows: {total_rows}, Alerts: {alerts}")

    spark.stop()

if __name__ == "__main__":
    main()