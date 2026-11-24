from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import os
import sys
import json

# Ensure PySpark uses the active Python executable and Hadoop winutils on Windows
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
if not os.environ.get("HADOOP_HOME"):
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["hadoop.home.dir"] = "C:\\hadoop"
if r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

BASE = os.path.dirname(os.path.abspath(__file__))
INPUT_CSV = os.path.join(BASE, "processed_data", "youtube_data_clean.csv")
OUTPUT_DIR = os.path.join(BASE, "processed_data", "spark_outputs")


def create_spark_session(app_name="YouTubeSparkPipeline"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_dataset(spark):
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("multiLine", "true") \
        .option("escape", "\"") \
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
        .csv(INPUT_CSV)
    for col in ("Duration_Seconds", "Likes", "Engagement_Rate", "Duration_Minutes",
                "Duration_Hours", "Views_per_Minute", "Like_Rate", "Comment_Rate"):
        df = df.withColumn(col, F.expr("try_cast(`%s` as double)" % col))
    for col in ("Views", "Comments", "Published_Year", "Published_Month",
                "Published_DayOfWeek", "Published_Hour", "Published_Quarter"):
        df = df.withColumn(col, F.expr("try_cast(`%s` as bigint)" % col))
    df = df.withColumn("Published_Date", F.to_timestamp(F.col("Published_Date")))
    print(f"Loaded {df.count():,} rows x {len(df.columns)} columns from {INPUT_CSV}")
    bad_dates = df.filter(F.col("Published_Date").isNull()).count()
    print(f"Rows with unparseable Published_Date: {bad_dates:,}")
    return df


def apply_filters(df):
    filtered = df.filter(
        (F.col("Published_Date") >= F.lit("2024-01-01").cast("timestamp")) &
        (F.col("Published_Date") < F.lit("2026-01-01").cast("timestamp")) &
        (F.col("Views") > 0) &
        (F.col("Duration_Seconds") >= 0) &
        (F.col("Content_Type") != "Upcoming Premiere")
    )
    print(f"Filtering (2024-2025, views > 0, non-premiere): {filtered.count():,} rows retained")
    return filtered


def aggregate(df):
    channel_summary = df.groupBy("Channel").agg(
        F.count("*").alias("Video_Count"),
        F.sum("Views").alias("Total_Views"),
        F.sum("Likes").alias("Total_Likes"),
        F.sum("Comments").alias("Total_Comments"),
        F.percentile_approx("Views", 0.5).alias("Median_Views"),
        F.avg("Engagement_Rate").alias("Avg_Engagement_Rate"),
        F.avg("Views_per_Minute").alias("Avg_Views_per_Minute"),
        F.avg(F.when(F.col("Content_Type") == "Shorts", 1).otherwise(0)).alias("Shorts_Share"),
    ).orderBy(F.desc("Total_Views"))

    content_summary = df.groupBy("Content_Type").agg(
        F.count("*").alias("Video_Count"),
        F.sum("Views").alias("Total_Views"),
        F.percentile_approx("Views", 0.5).alias("Median_Views"),
        F.avg("Engagement_Rate").alias("Avg_Engagement_Rate"),
        F.avg("Views_per_Minute").alias("Avg_Views_per_Minute"),
    ).orderBy(F.desc("Total_Views"))

    monthly = df.withColumn("Year_Month", F.date_format("Published_Date", "yyyy-MM")).groupBy("Year_Month").agg(
        F.count("*").alias("Video_Count"),
        F.sum("Views").alias("Total_Views"),
        F.percentile_approx("Views", 0.5).alias("Median_Views"),
        F.avg("Engagement_Rate").alias("Avg_Engagement_Rate"),
    ).orderBy("Year_Month")

    hourly = df.groupBy("Published_Hour").agg(
        F.count("*").alias("Video_Count"),
        F.sum("Views").alias("Total_Views"),
        F.percentile_approx("Views", 0.5).alias("Median_Views"),
        F.avg("Engagement_Rate").alias("Avg_Engagement_Rate"),
    ).orderBy("Published_Hour")

    weekday = df.groupBy("Published_DayOfWeek").agg(
        F.count("*").alias("Video_Count"),
        F.sum("Views").alias("Total_Views"),
        F.percentile_approx("Views", 0.5).alias("Median_Views"),
        F.avg("Engagement_Rate").alias("Avg_Engagement_Rate"),
    ).orderBy("Published_DayOfWeek")

    engagement_band = df.groupBy("Engagement_Category").agg(
        F.count("*").alias("Video_Count"),
        F.sum("Views").alias("Total_Views"),
        F.avg("Engagement_Rate").alias("Avg_Engagement_Rate"),
    ).orderBy(F.desc("Total_Views"))

    print("Aggregation complete: channel, content type, month, hour, weekday, engagement band")
    return channel_summary, content_summary, monthly, hourly, weekday, engagement_band


def join_stage(df, channel_summary, content_summary, monthly):
    channel_stats = channel_summary.select(
        F.col("Channel"),
        F.col("Median_Views").alias("Channel_Median_Views"),
        F.col("Total_Views").alias("Channel_Total_Views"),
        F.col("Avg_Engagement_Rate").alias("Channel_Avg_Engagement"),
    )
    content_stats = content_summary.select(
        F.col("Content_Type"),
        F.col("Median_Views").alias("Content_Median_Views"),
        F.col("Avg_Engagement_Rate").alias("Content_Avg_Engagement"),
    )

    top_videos = df.orderBy(F.desc("Views")).limit(200)
    top_videos_enriched = top_videos \
        .join(F.broadcast(channel_stats), on="Channel", how="inner") \
        .join(F.broadcast(content_stats), on="Content_Type", how="inner") \
        .select(
            "Video_ID", "Title", "Channel", "Content_Type", "Published_Date",
            "Views", "Likes", "Comments", "Engagement_Rate",
            "Channel_Median_Views", "Channel_Total_Views", "Channel_Avg_Engagement",
            "Content_Median_Views", "Content_Avg_Engagement",
            (F.col("Views") / F.col("Channel_Median_Views")).alias("Views_vs_Channel_Median"),
            (F.col("Engagement_Rate") - F.col("Content_Avg_Engagement")).alias("Engagement_vs_Content"),
        ) \
        .orderBy(F.desc("Views"))

    channel_content = channel_summary.join(
        F.broadcast(content_stats),
        how="cross"
    ).withColumn(
        "Engagement_vs_Content",
        F.col("Avg_Engagement_Rate") - F.col("Content_Avg_Engagement")
    ).withColumn(
        "Median_vs_Content",
        F.col("Median_Views") - F.col("Content_Median_Views")
    )

    channel_month = df.withColumn("Year_Month", F.date_format("Published_Date", "yyyy-MM")) \
        .groupBy("Year_Month", "Channel").agg(
            F.count("*").alias("Video_Count"),
            F.sum("Views").alias("Total_Views"),
            F.percentile_approx("Views", 0.5).alias("Median_Views"),
            F.avg("Engagement_Rate").alias("Avg_Engagement_Rate"),
        )
    month_totals = monthly.select(
        F.col("Year_Month"),
        F.col("Total_Views").alias("Month_Total_Views"),
        F.col("Video_Count").alias("Month_Video_Count"),
    )
    channel_monthly = channel_month.join(
        F.broadcast(month_totals),
        on="Year_Month",
        how="inner"
    ).withColumn(
        "Share_of_Month_Views",
        F.col("Total_Views") / F.col("Month_Total_Views")
    ).orderBy("Year_Month", F.desc("Total_Views"))

    print("Join stage complete: top-video enrichment, content benchmark, channel x month share")
    return top_videos_enriched, channel_content, channel_monthly


def write_outputs(df, frames):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    names = [
        "channel_summary", "content_type_summary", "monthly_trends", "hourly_pattern",
        "weekday_pattern", "engagement_bands", "top_videos_enriched", "channel_content_benchmark",
        "channel_month_share",
    ]
    for name, frame in zip(names, frames):
        path = os.path.join(OUTPUT_DIR, f"{name}.csv")
        frame.coalesce(1).write.mode("overwrite").option("header", "true").csv(path + "_tmp")
        import glob
        import shutil
        parts = glob.glob(path + "_tmp/part-*.csv")
        if parts:
            if os.path.exists(path):
                os.remove(path)
            shutil.move(parts[0], path)
        shutil.rmtree(path + "_tmp", ignore_errors=True)
        print(f"Written: {path}")

    parquet_dir = os.path.join(OUTPUT_DIR, "fact_videos_partitioned")
    df.write.mode("overwrite").partitionBy("Channel", "Content_Type").parquet(parquet_dir)
    print(f"Partitioned parquet written: {parquet_dir}")


def main():
    spark = create_spark_session()
    try:
        df = load_dataset(spark)
        filtered = apply_filters(df)
        channel_summary, content_summary, monthly, hourly, weekday, engagement_band = aggregate(filtered)
        top_videos, channel_content, channel_monthly = join_stage(filtered, channel_summary, content_summary, monthly)

        write_outputs(filtered, [
            channel_summary, content_summary, monthly, hourly, weekday, engagement_band,
            top_videos, channel_content, channel_monthly,
        ])

        print("\n--- Channel Performance (top 10 by total views) ---")
        channel_summary.show(10, truncate=False)
        print("\n--- Content Type Performance ---")
        content_summary.show(10, truncate=False)
        print("\n--- Best Publishing Hours (median views) ---")
        hourly.orderBy(F.desc("Median_Views")).show(5, truncate=False)
        print("\n--- Weekly Pattern ---")
        weekday.show(7, truncate=False)

        highlights = {
            "rows_scanned": df.count(),
            "rows_after_filter": filtered.count(),
            "top_channel_by_views": channel_summary.first().asDict(),
            "best_hour_by_median_views": hourly.orderBy(F.desc("Median_Views")).first().asDict(),
            "best_weekday_by_median_views": weekday.orderBy(F.desc("Median_Views")).first().asDict(),
            "content_types": [r.asDict() for r in content_summary.collect()],
        }
        with open(os.path.join(OUTPUT_DIR, "pipeline_highlights.json"), "w") as fh:
            json.dump(highlights, fh, indent=2, default=str)
        print("\nHighlights saved to pipeline_highlights.json")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
