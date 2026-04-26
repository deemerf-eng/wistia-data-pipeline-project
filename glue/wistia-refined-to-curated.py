import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "S3_BUCKET"]
)

S3_BUCKET = args["S3_BUCKET"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

refined_base = f"s3://{S3_BUCKET}/refined"
curated_base = f"s3://{S3_BUCKET}/curated"

inventory = spark.read.parquet(f"{refined_base}/media_inventory/")
stats = spark.read.parquet(f"{refined_base}/media_stats/")
engagement = spark.read.parquet(f"{refined_base}/media_engagement/")

engagement_avg = (
    engagement
    .groupBy("media_id")
    .agg(F.avg("engagement").alias("avg_engagement"))
)

media_perf = (
    stats
    .join(inventory, "media_id", "left")
    .join(engagement_avg, "media_id", "left")
    .withColumn(
        "avg_watch_time",
        F.when(F.col("play_count") > 0, F.col("hours_watched") / F.col("play_count"))
         .otherwise(F.lit(0))
    )
)

media_perf.write.mode("overwrite").parquet(f"{curated_base}/media_performance/")

engagement_summary = (
    engagement
    .groupBy("media_id")
    .agg(
        F.avg("engagement_value").alias("avg_engagement"),
        F.max("engagement_value").alias("max_engagement")
    )
)

engagement_summary.write.mode("overwrite").parquet(
    f"{curated_base}/engagement_summary/"
)
# =========================
# 3. MEDIA RETENTION (NEW)
# =========================
retention = spark.read.parquet(f"{refined_base}/media_engagement/")

retention_df = (
    retention
    .select(
        "media_id",
        "position_index",
        "engagement_value",
        "rewatch_value"
    )
)

retention_df.write.mode("overwrite").parquet(
    f"{curated_base}/media_retention/"
)
job.commit()
