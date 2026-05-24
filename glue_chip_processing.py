"""
AWS Glue PySpark Job — Chip Test Processing
Reads raw JSON from S3, validates, enriches, writes partitioned Parquet.
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'RAW_BUCKET', 'PROCESSED_BUCKET', 'DLQ_BUCKET'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

RAW_PATH = f"s3://{args['RAW_BUCKET']}/chip_tests/"
PROCESSED_PATH = f"s3://{args['PROCESSED_BUCKET']}/chip_tests/"
DLQ_PATH = f"s3://{args['DLQ_BUCKET']}/chip_tests/"

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("chip_id", StringType(), True),
    StructField("chip_type", StringType(), True),
    StructField("fab", StringType(), True),
    StructField("test_type", StringType(), True),
    StructField("measurement", DoubleType(), True),
    StructField("passed", BooleanType(), True),
    StructField("timestamp", StringType(), True),
    StructField("operator_id", StringType(), True),
])

raw_df = spark.read.schema(schema).json(RAW_PATH)
raw_count = raw_df.count()
print(f"Read {raw_count} raw events from {RAW_PATH}")

validated = raw_df.filter(
    F.col("event_id").isNotNull() &
    F.col("chip_id").rlike("^[A-Z][0-9]+-[0-9]+$") &
    F.col("measurement").isNotNull() &
    (F.col("measurement") >= 0) &
    F.col("fab").isin(["TSMC_N4", "TSMC_N5", "Samsung_3nm"])
)

bad_rows = raw_df.subtract(validated)
bad_count = bad_rows.count()
if bad_count > 0:
    print(f"Routing {bad_count} bad rows to DLQ")
    bad_rows.write.mode("append").json(DLQ_PATH)
else:
    print("No bad rows detected")

print(f"Validated rows: {validated.count()}")

chip_family = spark.createDataFrame([
    ("A100", "Ampere", "2020"),
    ("H100", "Hopper", "2022"),
    ("H200", "Hopper", "2023"),
    ("B100", "Blackwell", "2024"),
], ["chip_type", "family", "launch_year"])

enriched = validated.join(F.broadcast(chip_family), "chip_type", "left")

final = (enriched
    .withColumn("event_ts", F.to_timestamp("timestamp"))
    .withColumn("event_date", F.to_date("event_ts"))
)

(final.write
    .mode("overwrite")
    .partitionBy("fab", "event_date")
    .option("compression", "snappy")
    .parquet(PROCESSED_PATH)
)

print(f"Wrote partitioned Parquet to {PROCESSED_PATH}")

job.commit()
