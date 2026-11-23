from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, IndexToString
from pyspark.ml.clustering import KMeans
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import matplotlib.pyplot as plt
import json
import os
import sys
import warnings
warnings.filterwarnings('ignore')

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
if not os.environ.get("HADOOP_HOME"):
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["hadoop.home.dir"] = "C:\\hadoop"
if r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")


def create_spark_session(app_name="YouTubeMLAnalysis"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs") \
        .getOrCreate()


def load_data(spark, filepath):
    print(f"Loading data from {filepath} using PySpark...")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(filepath)
    print(f"Count: {df.count()}, Columns: {len(df.columns)}")
    return df


def prepare_features_for_clustering(df):
    feature_cols = ['Duration_Seconds', 'Views', 'Likes', 'Comments', 'Engagement_Rate',
                    'Published_Hour', 'Published_DayOfWeek', 'Published_Month',
                    'Is_Long_Form', 'Is_Short', 'Is_Live']

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw", handleInvalid="skip")
    df_assembled = assembler.transform(df).na.drop(subset=["features_raw"])

    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)

    return df_scaled, feature_cols, scaler_model


def perform_kmeans_clustering(df_scaled, n_clusters=5):
    print(f"\nPerforming K-Means clustering with {n_clusters} clusters...")
    kmeans = KMeans(featuresCol="features", predictionCol="Cluster", k=n_clusters, seed=42, initSteps=5)
    model = kmeans.fit(df_scaled)
    df_clustered = model.transform(df_scaled)

    cluster_sizes = df_clustered.groupBy("Cluster").count().orderBy("Cluster").collect()
    print(f"Cluster sizes: { {row['Cluster']: row['count'] for row in cluster_sizes} }")

    return df_clustered, model


def analyze_clusters(df_clustered, feature_cols):
    numeric_cols = ['Views', 'Likes', 'Comments', 'Engagement_Rate', 'Duration_Seconds',
                    'Is_Long_Form', 'Is_Short', 'Is_Live']

    agg_exprs = []
    for col in numeric_cols:
        agg_exprs.append(F.mean(col).alias(f"{col}_mean"))
        agg_exprs.append(F.expr(f"percentile_approx({col}, 0.5)").alias(f"{col}_median"))
        agg_exprs.append(F.stddev(col).alias(f"{col}_std"))

    for col in ['Published_Hour', 'Published_DayOfWeek']:
        agg_exprs.append(F.expr(f"percentile_approx({col}, 0.5)").cast("int").alias(f"{col}_mode"))

    for col in ['Channel', 'Content_Type']:
        agg_exprs.append(F.first(col).alias(f"{col}_mode"))

    agg_exprs.append(F.count("*").alias("Video_Count"))

    cluster_summary = df_clustered.groupBy("Cluster").agg(*agg_exprs).orderBy("Cluster")

    print("\n--- Cluster Analysis ---")
    cluster_summary.show(truncate=False)

    summary_dict = [row.asDict() for row in cluster_summary.collect()]

    return df_clustered, summary_dict


def prepare_ml_features(df):
    indexer_channel = StringIndexer(inputCol="Channel", outputCol="Channel_Encoded", handleInvalid="keep")
    indexer_content = StringIndexer(inputCol="Content_Type", outputCol="Content_Type_Encoded", handleInvalid="keep")

    pipeline = Pipeline(stages=[indexer_channel, indexer_content])
    model = pipeline.fit(df)
    df_indexed = model.transform(df)

    feature_cols = ['Duration_Seconds', 'Published_Hour', 'Published_DayOfWeek',
                    'Published_Month', 'Is_Long_Form', 'Is_Short', 'Is_Live',
                    'Channel_Encoded', 'Content_Type_Encoded']

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_ml", handleInvalid="skip")
    df_features = assembler.transform(df_indexed).na.drop(subset=["features_ml"])

    return df_features, feature_cols, model


def train_regression_models(df_features, feature_cols):
    print("\n--- Training Regression Models ---")

    targets = ['Views', 'Engagement_Rate', 'Likes']
    results = {}

    for target in targets:
        print(f"\nTraining Random Forest for {target}...")

        train, test = df_features.randomSplit([0.8, 0.2], seed=42)

        rf = RandomForestRegressor(featuresCol="features_ml", labelCol=target,
                                    numTrees=100, maxDepth=15, seed=42)

        model = rf.fit(train)
        predictions = model.transform(test)

        evaluator_rmse = RegressionEvaluator(labelCol=target, predictionCol="prediction", metricName="rmse")
        evaluator_r2 = RegressionEvaluator(labelCol=target, predictionCol="prediction", metricName="r2")

        rmse = evaluator_rmse.evaluate(predictions)
        r2 = evaluator_r2.evaluate(predictions)
        mse = rmse ** 2

        feature_importance = list(zip(feature_cols, model.featureImportances.toArray().tolist()))
        feature_importance = sorted(feature_importance, key=lambda x: x[1], reverse=True)

        results[target] = {
            'model': model,
            'mse': float(mse),
            'rmse': float(rmse),
            'r2': float(r2),
            'feature_importance': [{'Feature': f, 'Importance': float(imp)} for f, imp in feature_importance]
        }

        print(f"  RMSE: {rmse:.4f}")
        print(f"  R²: {r2:.4f}")
        print(f"  Top 3 features: {[f for f, _ in feature_importance[:3]]}")

    return results


def train_classification_model(df_features, feature_cols):
    print("\n--- Training Classification Model (High Engagement) ---")

    median_eng = df_features.approxQuantile("Engagement_Rate", [0.5], 0.01)[0]
    df_class = df_features.withColumn("High_Engagement", (F.col("Engagement_Rate") > median_eng).cast("int"))

    train, test = df_class.randomSplit([0.8, 0.2], seed=42)

    rf = RandomForestClassifier(featuresCol="features_ml", labelCol="High_Engagement",
                                numTrees=100, maxDepth=15, seed=42)

    model = rf.fit(train)
    predictions = model.transform(test)

    evaluator_acc = MulticlassClassificationEvaluator(labelCol="High_Engagement", predictionCol="prediction", metricName="accuracy")
    evaluator_prec = MulticlassClassificationEvaluator(labelCol="High_Engagement", predictionCol="prediction", metricName="weightedPrecision")
    evaluator_rec = MulticlassClassificationEvaluator(labelCol="High_Engagement", predictionCol="prediction", metricName="weightedRecall")
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="High_Engagement", predictionCol="prediction", metricName="f1")

    accuracy = evaluator_acc.evaluate(predictions)
    precision = evaluator_prec.evaluate(predictions)
    recall = evaluator_rec.evaluate(predictions)
    f1 = evaluator_f1.evaluate(predictions)

    feature_importance = list(zip(feature_cols, model.featureImportances.toArray().tolist()))
    feature_importance = sorted(feature_importance, key=lambda x: x[1], reverse=True)

    pred_and_labels = predictions.select("prediction", "High_Engagement").collect()
    y_true = [row.High_Engagement for row in pred_and_labels]
    y_pred = [int(row.prediction) for row in pred_and_labels]

    from sklearn.metrics import classification_report
    clf_report = classification_report(y_true, y_pred, output_dict=True, zero_division=0)

    print(f"Classification Report:")
    print(classification_report(y_true, y_pred, zero_division=0))

    print(f"Top 5 features for high engagement:")
    for f, imp in feature_importance[:5]:
        print(f"  {f}: {imp:.4f}")

    return {
        'model': model,
        'accuracy': float(accuracy),
        'precision': float(precision),
        'recall': float(recall),
        'f1': float(f1),
        'feature_importance': [{'Feature': f, 'Importance': float(imp)} for f, imp in feature_importance],
        'classification_report': clf_report
    }


def generate_visualizations(df_clustered_pd, results, cluster_summary, output_dir):
    print("\n--- Generating Visualizations ---")
    os.makedirs(output_dir, exist_ok=True)

    plt.style.use('seaborn-v0_8')

    fig, axes = plt.subplots(2, 2, figsize=(15, 12))

    cluster_counts = df_clustered_pd['Cluster'].value_counts().sort_index()
    axes[0, 0].bar(cluster_counts.index.astype(str), cluster_counts.values, color='skyblue', edgecolor='navy')
    axes[0, 0].set_title('Cluster Sizes')
    axes[0, 0].set_xlabel('Cluster')
    axes[0, 0].set_ylabel('Number of Videos')

    engagement_by_cluster = df_clustered_pd.groupby('Cluster')['Engagement_Rate'].mean()
    axes[0, 1].bar(engagement_by_cluster.index.astype(str), engagement_by_cluster.values, color='lightcoral', edgecolor='darkred')
    axes[0, 1].set_title('Average Engagement Rate by Cluster')
    axes[0, 1].set_xlabel('Cluster')
    axes[0, 1].set_ylabel('Avg Engagement Rate')

    views_by_content = df_clustered_pd.groupby('Content_Type')['Views'].median().sort_values(ascending=False)
    axes[1, 0].bar(range(len(views_by_content)), views_by_content.values, color='lightgreen', edgecolor='darkgreen')
    axes[1, 0].set_xticks(range(len(views_by_content)))
    axes[1, 0].set_xticklabels(views_by_content.index, rotation=45)
    axes[1, 0].set_title('Median Views by Content Type')
    axes[1, 0].set_ylabel('Median Views')

    engagement_by_channel = df_clustered_pd.groupby('Channel')['Engagement_Rate'].mean().sort_values(ascending=False)
    axes[1, 1].bar(range(len(engagement_by_channel)), engagement_by_channel.values, color='gold', edgecolor='orange')
    axes[1, 1].set_xticks(range(len(engagement_by_channel)))
    axes[1, 1].set_xticklabels(engagement_by_channel.index, rotation=45, ha='right')
    axes[1, 1].set_title('Average Engagement Rate by Channel')
    axes[1, 1].set_ylabel('Avg Engagement Rate')

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'cluster_analysis.png'), dpi=300, bbox_inches='tight')
    plt.close()

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    for i, (target_name, res) in enumerate(results.items()):
        fi = pd.DataFrame(res['feature_importance']).head(8)
        axes[i].barh(range(len(fi)), fi['Importance'], color='steelblue')
        axes[i].set_yticks(range(len(fi)))
        axes[i].set_yticklabels(fi['Feature'])
        axes[i].set_title(f'Feature Importance: {target_name}\n(R² = {res["r2"]:.3f})')
        axes[i].invert_yaxis()

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'feature_importance.png'), dpi=300, bbox_inches='tight')
    plt.close()

    print(f"Visualizations saved to {output_dir}")


def save_ml_results(cluster_summary, regression_results, classification_results, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    ml_results = {
        'cluster_analysis': cluster_summary,
        'regression_models': {
            target: {
                'mse': res['mse'],
                'rmse': res.get('rmse', 0),
                'r2': res['r2'],
                'feature_importance': res['feature_importance']
            } for target, res in regression_results.items()
        },
        'classification_model': {
            'accuracy': classification_results['accuracy'],
            'precision': classification_results['precision'],
            'recall': classification_results['recall'],
            'f1': classification_results['f1'],
            'feature_importance': classification_results['feature_importance'],
            'classification_report': classification_results['classification_report']
        }
    }

    with open(os.path.join(output_dir, 'ml_results.json'), 'w') as f:
        json.dump(ml_results, f, indent=2, default=str)

    print(f"ML results saved to {output_dir}/ml_results.json")


def generate_insights(df_pd, df_clustered_pd, cluster_summary, regression_results, classification_results):
    print("\n" + "="*60)
    print("KEY INSIGHTS FROM ML ANALYSIS")
    print("="*60)

    print("\n1. CLUSTER INSIGHTS:")
    for cluster_id in sorted(df_clustered_pd['Cluster'].unique()):
        subset = df_clustered_pd[df_clustered_pd['Cluster'] == cluster_id]
        size = len(subset)
        pct = size / len(df_clustered_pd) * 100
        avg_views = subset['Views'].mean()
        avg_eng = subset['Engagement_Rate'].mean()
        top_content = subset['Content_Type'].mode().iloc[0] if not subset['Content_Type'].mode().empty else 'N/A'
        top_channel = subset['Channel'].mode().iloc[0] if not subset['Channel'].mode().empty else 'N/A'
        print(f"   Cluster {cluster_id} ({size:,} videos, {pct:.1f}%): "
              f"Avg Views={avg_views:,.0f}, Avg Engagement={avg_eng:.3f}, "
              f"Dominant Content={top_content}, Top Channel={top_channel}")

    print("\n2. REGRESSION MODEL PERFORMANCE:")
    for target, res in regression_results.items():
        print(f"   {target}: R² = {res['r2']:.3f}, MSE = {res['mse']:.2f}")
        top_feat = res['feature_importance'][0]
        print(f"      Most important feature: {top_feat['Feature']} ({top_feat['Importance']:.3f})")

    print("\n3. HIGH ENGAGEMENT CLASSIFICATION:")
    clf_report = classification_results['classification_report']
    print(f"   Accuracy: {classification_results['accuracy']:.3f}")
    print(f"   Precision (High Engagement): {clf_report.get('1', {}).get('precision', 'N/A'):.3f}")
    print(f"   Recall (High Engagement): {clf_report.get('1', {}).get('recall', 'N/A'):.3f}")
    top_feat = classification_results['feature_importance'][0]
    print(f"   Top predictor of high engagement: {top_feat['Feature']} ({top_feat['Importance']:.3f})")

    print("\n4. CONTENT TYPE PERFORMANCE:")
    content_perf = df_pd.groupby('Content_Type').agg({
        'Views': 'median',
        'Engagement_Rate': 'mean',
        'Video_ID': 'count'
    }).round(2)
    content_perf.columns = ['Median_Views', 'Avg_Engagement', 'Count']
    print(content_perf.to_string())

    print("\n5. CHANNEL PERFORMANCE:")
    channel_perf = df_pd.groupby('Channel').agg({
        'Views': 'median',
        'Engagement_Rate': 'mean',
        'Likes': 'median',
        'Video_ID': 'count'
    }).round(2)
    channel_perf.columns = ['Median_Views', 'Avg_Engagement', 'Median_Likes', 'Count']
    channel_perf = channel_perf.sort_values('Median_Views', ascending=False)
    print(channel_perf.head(10).to_string())

    print("\n6. TEMPORAL PATTERNS:")
    hourly_views = df_pd.groupby('Published_Hour')['Views'].median()
    best_hour = hourly_views.idxmax()
    worst_hour = hourly_views.idxmin()
    print(f"   Best publishing hour: {best_hour}:00 (median views: {hourly_views[best_hour]:,.0f})")
    print(f"   Worst publishing hour: {worst_hour}:00 (median views: {hourly_views[worst_hour]:,.0f})")

    dow_engagement = df_pd.groupby('Published_DayOfWeek')['Engagement_Rate'].mean()
    dow_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    best_dow = dow_engagement.idxmax()
    worst_dow = dow_engagement.idxmin()
    print(f"   Best day for engagement: {dow_names[best_dow]} ({dow_engagement[best_dow]:.3f})")
    print(f"   Worst day for engagement: {dow_names[worst_dow]} ({dow_engagement[worst_dow]:.3f})")


if __name__ == "__main__":
    import pandas as pd

    spark = create_spark_session()
    try:
        input_file = "D:/bigdataproject/PROJECT/processed_data/youtube_data_clean.csv"
        output_dir = "D:/bigdataproject/ml_results"

        df = load_data(spark, input_file)

        df_scaled, feature_cols, scaler = prepare_features_for_clustering(df)
        df_clustered, kmeans_model = perform_kmeans_clustering(df_scaled, n_clusters=5)
        df_clustered, cluster_summary = analyze_clusters(df_clustered, feature_cols)

        df_features, feature_cols_ml, indexer_model = prepare_ml_features(df_clustered)
        regression_results = train_regression_models(df_features, feature_cols_ml)
        classification_results = train_classification_model(df_features, feature_cols_ml)

        df_clustered_pd = df_clustered.select("*").toPandas()
        df_pd = df.select("*").toPandas()

        generate_visualizations(df_clustered_pd, regression_results, cluster_summary, output_dir)
        save_ml_results(cluster_summary, regression_results, classification_results, output_dir)
        generate_insights(df_pd, df_clustered_pd, cluster_summary, regression_results, classification_results)

        df_clustered_pd.to_csv(os.path.join(output_dir, 'youtube_data_with_clusters.csv'), index=False)
        print(f"\nClustered data saved to {output_dir}/youtube_data_with_clusters.csv")

        print("\n--- ML Analysis Complete ---")
    finally:
        spark.stop()