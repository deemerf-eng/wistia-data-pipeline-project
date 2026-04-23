import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# =========================
# Arguments
# =========================
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "S3_BUCKET", "RAW_PREFIX", "REFINED_PREFIX"]
)

S3_BUCKET = args["S3_BUCKET"]
RAW_PREFIX = args["RAW_PREFIX"].strip("/")
REFINED_PREFIX = args["REFINED_PREFIX"].strip("/")

# =========================
# Spark Setup
# =========================
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# =========================
# Paths
# =========================
raw_base = f"s3://{S3_BUCKET}/{RAW_PREFIX}"
refined_base = f"s3://{S3_BUCKET}/{REFINED_PREFIX}"

# =========================
# 1. MEDIA INVENTORY
# =========================
inventory = spark.read.option("multiLine", "true").json(f"{raw_base}/media_inventory/")

inventory_df = (
    inventory
    .withColumn("media_id", F.col("hashed_id"))
    .withColumn("folder_name", F.col("folder.name"))
    .select(
        "media_id",
        "id",
        "name",
        "duration",
        "created",
        "updated",
        "type",
        "archived",
        "status",
        "progress",
        "folder_name"
    )
)

inventory_df.write.mode("overwrite").parquet(f"{refined_base}/media_inventory/")

# =========================
# 2. MEDIA STATS BY DATE
# =========================
stats = spark.read.option("multiLine", "true").json(f"{raw_base}/media_stats_by_date/")

stats_df = (
    stats
    .withColumn(
        "media_id",
        F.regexp_extract(F.input_file_name(), r"media_id=([^.]+)\.json", 1)
    )
    .select(
        "media_id",
        "date",
        "load_count",
        "play_count",
        "hours_watched"
    )
)

stats_df.write.mode("overwrite").parquet(f"{refined_base}/media_stats/")

# =========================
# 3. MEDIA ENGAGEMENT
# =========================
engagement = spark.read.option("multiLine", "true").json(f"{raw_base}/media_engagement/")

engagement_df = (
    engagement
    .withColumn(
        "media_id",
        F.regexp_extract(F.input_file_name(), r"media_id=([^.]+)\.json", 1)
    )
)

engagement_exploded = (
    engagement_df
    .select(
        "media_id",
        "engagement",
        F.posexplode("engagement_data").alias("position_index", "engagement_value"),
        "rewatch_data"
    )
    .withColumn(
        "rewatch_value",
        F.col("rewatch_data")[F.col("position_index")]
    )
    .select(
        "media_id",
        "engagement",
        "position_index",
        "engagement_value",
        "rewatch_value"
    )
)

engagement_exploded.write.mode("overwrite").parquet(
    f"{refined_base}/media_engagement/"
)

job.commit()
