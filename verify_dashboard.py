from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import sys

# Ensure PySpark uses the active Python executable and Hadoop winutils on Windows
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
if not os.environ.get("HADOOP_HOME"):
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["hadoop.home.dir"] = "C:\\hadoop"
if r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")


def create_spark_session(app_name="VerifyDashboard"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs") \
        .getOrCreate()


if __name__ == "__main__":
    spark = create_spark_session()
    try:
        data_path = "D:/bigdataproject/processed_data/youtube_data_clean.csv"
        if not os.path.exists(data_path):
            data_path = "D:/bigdataproject/PROJECT/processed_data/youtube_data_clean.csv"

        print(f"Loading data from {data_path} using PySpark...")
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(data_path)

        totals = df.agg(
            F.sum("Views").alias("total_views"),
            F.sum("Likes").alias("total_likes"),
            F.sum("Comments").alias("total_comments"),
            F.mean("Duration_Seconds").alias("avg_duration"),
            F.mean("Engagement_Rate").alias("avg_engagement")
        ).collect()[0]

        print('Total Views:', format(int(totals['total_views']), ','))
        print('Total Likes:', format(int(totals['total_likes']), ','))
        print('Total Comments:', format(int(totals['total_comments']), ','))
        print('Avg Duration_Seconds:', round(totals['avg_duration'], 2))
        print('Avg Engagement_Rate:', round(totals['avg_engagement'], 4))
        print()

        print('Views by Engagement_Category:')
        agg = df.groupBy("Engagement_Category").agg(F.sum("Views").alias("total_views")).orderBy("Engagement_Category")
        for row in agg.collect():
            print(' ', row['Engagement_Category'], ':', format(int(row['total_views']), ','))
        print()

        print('Views by Content_Type:')
        ct = df.groupBy("Content_Type").agg(F.sum("Views").alias("total_views")).orderBy(F.desc("total_views"))
        for row in ct.collect():
            print(' ', row['Content_Type'], ':', format(int(row['total_views']), ','))
        print()

        print('Views by Year:')
        yr = df.groupBy("Published_Year").agg(F.sum("Views").alias("total_views")).orderBy("Published_Year")
        for row in yr.collect():
            print(' ', row['Published_Year'], ':', format(int(row['total_views']), ','))
        print()

        print('Monthly views (sample of line chart):')
        monthly = df.withColumn("YearMonth", F.date_format("Published_Date", "yyyy-MM")) \
            .groupBy("YearMonth").agg(F.sum("Views").alias("total_views")) \
            .orderBy("YearMonth")
        for row in monthly.limit(12).collect():
            print(' ', row['YearMonth'], ':', format(int(row['total_views']), ','))
    finally:
        spark.stop()
