from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
import os
import sys
import json
import glob
import shutil

# Ensure PySpark uses the active Python executable and Hadoop winutils on Windows
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
if not os.environ.get("HADOOP_HOME"):
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["hadoop.home.dir"] = "C:\\hadoop"
if r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")


def create_spark_session(app_name="YouTubeDataPreprocessing"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs") \
        .getOrCreate()


def load_and_clean_data(spark, input_path, output_dir):
    print(f"Loading data from {input_path} using PySpark...")
    # Load dataset natively with PySpark CSV reader
    df = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("escape", "\"") \
        .csv(input_path)

    # Cast numeric columns to proper PySpark types
    df = df.withColumn("Duration_Seconds", F.col("Duration_Seconds").cast(T.DoubleType())) \
           .withColumn("Views", F.col("Views").cast(T.LongType())) \
           .withColumn("Likes", F.col("Likes").cast(T.DoubleType())) \
           .withColumn("Comments", F.col("Comments").cast(T.LongType())) \
           .withColumn("Engagement_Rate", F.col("Engagement_Rate").cast(T.DoubleType()))

    print(f"Original count: {df.count()}, columns: {len(df.columns)}")

    print("\n--- Data Quality Checks ---")
    missing_counts = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).collect()[0].asDict()
    print(f"Missing values: {missing_counts}")

    duplicate_vids = df.groupBy("Video_ID").count().filter(F.col("count") > 1).count()
    duplicate_rows = df.count() - df.dropDuplicates().count()
    print(f"Duplicate Video_IDs: {duplicate_vids}")
    print(f"Duplicate rows: {duplicate_rows}")

    df_clean = df.dropDuplicates()

    print("\n--- Feature Engineering (Native PySpark) ---")
    # Native PySpark date and time parsing in UTC
    df_clean = df_clean.withColumn("Published_Date", F.to_timestamp(F.col("Published_Date")))

    df_clean = df_clean.withColumn("Published_Year", F.year("Published_Date")) \
        .withColumn("Published_Month", F.month("Published_Date")) \
        .withColumn("Published_DayOfWeek", (F.dayofweek("Published_Date") + 5) % 7) \
        .withColumn("Published_Hour", F.hour("Published_Date")) \
        .withColumn("Published_Quarter", F.quarter("Published_Date"))

    df_clean = df_clean.withColumn("Duration_Minutes", F.col("Duration_Seconds") / 60.0) \
        .withColumn("Duration_Hours", F.col("Duration_Seconds") / 3600.0)

    df_clean = df_clean.withColumn("Views_per_Minute",
        F.when(F.col("Duration_Minutes") > 0, F.col("Views") / F.col("Duration_Minutes"))
        .otherwise(F.col("Views").cast(T.DoubleType()))
    )

    df_clean = df_clean.withColumn("Like_Rate",
        F.when(F.col("Views") > 0, F.col("Likes") / F.col("Views"))
        .otherwise(0.0)
    )

    df_clean = df_clean.withColumn("Comment_Rate",
        F.when(F.col("Views") > 0, F.col("Comments") / F.col("Views"))
        .otherwise(0.0)
    )

    df_clean = df_clean.withColumn("Is_Long_Form", (F.col("Duration_Seconds") > 600).cast("int")) \
        .withColumn("Is_Short", (F.col("Content_Type") == "Shorts").cast("int")) \
        .withColumn("Is_Live", F.col("Content_Type").isin(["Live Stream (Archived)", "Live Now"]).cast("int"))

    df_clean = df_clean.withColumn("Engagement_Category",
        F.when(F.col("Engagement_Rate") <= 0.5, "Low")
        .when(F.col("Engagement_Rate") <= 1.0, "Medium")
        .when(F.col("Engagement_Rate") <= 2.0, "High")
        .otherwise("Viral")
    )

    view_quantiles = df_clean.approxQuantile("Views", [0.2, 0.4, 0.6, 0.8], 0.01)
    df_clean = df_clean.withColumn("View_Category",
        F.when(F.col("Views") <= view_quantiles[0], "Very Low")
        .when(F.col("Views") <= view_quantiles[1], "Low")
        .when(F.col("Views") <= view_quantiles[2], "Medium")
        .when(F.col("Views") <= view_quantiles[3], "High")
        .otherwise("Very High")
    )

    new_cols = [c for c in df_clean.columns if c not in df.columns]
    print(f"\nNew columns: {new_cols}")
    cleaned_count = df_clean.count()
    print(f"Cleaned count: {cleaned_count}")

    os.makedirs(output_dir, exist_ok=True)

    # 1. Native PySpark CSV write
    csv_path = os.path.join(output_dir, 'youtube_data_clean.csv')
    csv_temp_dir = os.path.join(output_dir, '_csv_temp_export')
    try:
        df_clean.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
            .option("escape", "\"") \
            .csv(csv_temp_dir)
        part_files = glob.glob(os.path.join(csv_temp_dir, "part-*.csv"))
        if part_files:
            if os.path.exists(csv_path):
                os.remove(csv_path)
            shutil.move(part_files[0], csv_path)
            shutil.rmtree(csv_temp_dir, ignore_errors=True)
            print(f"CSV saved via PySpark to {csv_path}")
    except Exception as e:
        print(f"CSV export via PySpark encountered an error: {e}")

    # 2. Native PySpark Parquet write
    parquet_path = os.path.join(output_dir, 'youtube_data_clean.parquet')
    try:
        df_clean.write.mode("overwrite").parquet(parquet_path)
        print(f"Parquet saved via PySpark to {parquet_path}")
    except Exception as e:
        print(f"Parquet skipped: {e}")

    return df_clean


def generate_summary_stats(df, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    total_videos = df.count()
    total_channels = df.select("Channel").distinct().count()

    date_range_row = df.agg(F.min("Published_Date").alias("min_date"), F.max("Published_Date").alias("max_date")).collect()[0]
    min_date = date_range_row['min_date'].date() if date_range_row['min_date'] else "N/A"
    max_date = date_range_row['max_date'].date() if date_range_row['max_date'] else "N/A"
    date_range = f"{min_date} to {max_date}"

    agg_row = df.agg(
        F.sum("Views").alias("total_views"),
        F.sum("Likes").alias("total_likes"),
        F.sum("Comments").alias("total_comments"),
        F.mean("Engagement_Rate").alias("avg_engagement_rate")
    ).collect()[0]

    content_type_dist = df.groupBy("Content_Type").count().orderBy(F.desc("count")).collect()
    channel_dist = df.groupBy("Channel").count().orderBy(F.desc("count")).collect()

    top_10_views = df.orderBy(F.desc("Views")).limit(10).select("Video_ID", "Title", "Channel", "Views", "Engagement_Rate").collect()
    top_10_engagement = df.orderBy(F.desc("Engagement_Rate")).limit(10).select("Video_ID", "Title", "Channel", "Views", "Engagement_Rate").collect()

    summary = {
        'total_videos': total_videos,
        'total_channels': total_channels,
        'date_range': date_range,
        'total_views': int(agg_row['total_views']) if agg_row['total_views'] is not None else 0,
        'total_likes': int(agg_row['total_likes']) if agg_row['total_likes'] is not None else 0,
        'total_comments': int(agg_row['total_comments']) if agg_row['total_comments'] is not None else 0,
        'avg_engagement_rate': float(agg_row['avg_engagement_rate']) if agg_row['avg_engagement_rate'] is not None else 0.0,
        'content_type_distribution': {row['Content_Type']: row['count'] for row in content_type_dist},
        'channel_distribution': {row['Channel']: row['count'] for row in channel_dist},
        'top_10_by_views': [row.asDict() for row in top_10_views],
        'top_10_by_engagement': [row.asDict() for row in top_10_engagement],
    }

    with open(os.path.join(output_dir, 'summary_stats.json'), 'w') as f:
        json.dump(summary, f, indent=2, default=str)

    print("Summary statistics saved.")
    return summary


if __name__ == "__main__":
    spark = create_spark_session()
    try:
        project_dir = os.path.dirname(os.path.abspath(__file__))
        candidates = [
            os.path.join(project_dir, "b.CSV"),
            "D:/bigdataproject/b.CSV",
            "D:/bigdataproject/PROJECT/b.CSV",
        ]
        input_file = next((p for p in candidates if os.path.exists(p)), None)
        if input_file is None:
            raise FileNotFoundError(f"b.CSV not found in any of: {candidates}")
        output_dir = os.path.join(project_dir, "processed_data")

        df_clean = load_and_clean_data(spark, input_file, output_dir)
        summary = generate_summary_stats(df_clean, output_dir)

        print("\n--- Preprocessing Complete ---")
        print(f"Processed {df_clean.count()} records")
        print(f"Input: {input_file}")
        print(f"Output directory: {output_dir}")
    finally:
        spark.stop()
