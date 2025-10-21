import sys
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pyspark.sql.functions import (
    col, avg, max, round, when, unix_timestamp, from_unixtime,
    date_format, ceil, floor, explode, sequence, lit
)

# --- Configuration ---
THRESHOLDS_FILE = "thresholds.txt"
WINDOW_SIZE = 30
SLIDE = 10
START_OFFSET = 0
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
        print(f"❌ Error reading thresholds file: {e}")
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
    net_thresh = thresholds.get("net_in_threshold", 99999.0)
    disk_thresh = thresholds.get("disk_io_threshold", 99999.0)

    spark = SparkSession.builder.appName("SparkJob2_NET_DISK_Processing").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("🚀 SparkJob2 starting: Processing NET and DISK data...")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    net_csv = os.path.join(script_dir, "net_data.csv")
    disk_csv = os.path.join(script_dir, "disk_data.csv")

    # --- Schemas for CSVs ---
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

    # --- Read CSVs ---
    net_df = spark.read.option("header", "true").schema(net_schema).csv(net_csv)
    disk_df = spark.read.option("header", "true").schema(disk_schema).csv(disk_csv)

    # --- Convert timestamps ---
    net_df = net_df.withColumn("ts", from_unixtime(floor(unix_timestamp("ts"))).cast(TimestampType()))
    disk_df = disk_df.withColumn("ts", from_unixtime(floor(unix_timestamp("ts"))).cast(TimestampType()))

    # --- Join and prep ---
    combined_df = net_df.join(disk_df, ["server_id", "ts"], "inner") \
        .select("server_id", "ts", "net_in", "net_out", "disk_io") \
        .withColumn("ts_sec", unix_timestamp("ts"))

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
    combined_df = combined_df.withColumn("k", explode(sequence(col("k_min"), col("k_max"))))
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
        round(avg("net_in"), 2).alias("avg_net_in"),
        round(avg("disk_io"), 2).alias("avg_disk_io"),
        max("net_in").alias("max_net_in"),
        max("disk_io").alias("max_disk_io")
    )

    # --- Alert logic ---
    final_df = agg_df.select(
        "server_id",
        date_format(col("window_start"), "HH:mm:ss").alias("window_start"),
        date_format(col("window_end"), "HH:mm:ss").alias("window_end"),
        "avg_net_in", "avg_disk_io", "max_net_in", "max_disk_io",
        when(
            (col("max_net_in") > net_thresh) & (col("max_disk_io") > disk_thresh),
            "Network flood + Disk thrash suspected"
        ).when(
            col("max_net_in") > net_thresh,
            "Possible DDoS"
        ).when(
            col("max_disk_io") > disk_thresh,
            "Disk thrash suspected"
        ).otherwise(lit("")).alias("alert")
    )

    final_df = final_df.dropDuplicates(["server_id", "window_start", "window_end"]).orderBy("server_id", "window_start")

    output_path = os.path.join(script_dir, "team_87_NET_DISK.csv")
    write_single_csv(final_df, output_path)

    total_rows = final_df.count()
    alerts = final_df.filter(col("alert") != "").count()
    print(f"✅ SparkJob2 complete. Output file: {output_path}")
    print(f"✅ Total rows: {total_rows}, Alerts: {alerts}")

    spark.stop()

if __name__ == "__main__":
    main()