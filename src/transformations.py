import os
import re
import logging
import shutil
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, trim, regexp_replace, when, udf
from pyspark.sql.types import StringType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PARQUET_COMPRESSION = "snappy"
PARTITION_NAME_MAX_LENGTH = 255

def get_spark_session(app_name="BreweryPipeline"):
    """
    Creates or gets a SparkSession with optimized configurations.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
        .config("spark.sql.parquet.compression.codec", PARQUET_COMPRESSION) \
        .getOrCreate()

def sanitize_partition_name(value):
    """Sanitizes partition values to be filesystem-safe."""
    if value is None:
        return "Unknown"
    
    value = str(value).strip()
    value = re.sub(r'[/\\:*?"<>|]', '_', value)  # Filesystem-unsafe chars
    value = re.sub(r'[\x00-\x1f\x7f]', '', value)  # Control characters
    value = re.sub(r'_+', '_', value)  # Multiple underscores to single
    
    if len(value) > PARTITION_NAME_MAX_LENGTH:
        value = value[:PARTITION_NAME_MAX_LENGTH]
    
    return value if value else "Unknown"

def validate_schema(df, expected_columns=None):
    """Validates DataFrame schema before writing."""
    if df is None or df.rdd.isEmpty():
        raise ValueError("DataFrame is empty or None")
    
    for field in df.schema.fields:
        if hasattr(field.dataType, 'elementType') or hasattr(field.dataType, 'valueType'):
            logger.warning(f"Field '{field.name}' has complex type {field.dataType}")
    
    if expected_columns:
        missing_cols = set(expected_columns) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
    
    logger.info(f"Schema validation passed. Columns: {df.columns}")
    return True

def process_bronze_to_silver(bronze_file, silver_root):
    """Reads Bronze JSON, transforms to Silver Parquet partitioned by state."""
    spark = get_spark_session("BronzeToSilver")
    
    if not os.path.exists(bronze_file):
        raise FileNotFoundError(f"Bronze file not found: {bronze_file}")
    
    logger.info(f"Reading Bronze data from {bronze_file}")
    df = spark.read.json(bronze_file, multiLine=True)
    logger.info(f"Read {df.count()} records from Bronze layer")
    
    validate_schema(df)
    
    # Ensure state column exists and handle nulls
    if 'state' not in df.columns:
        logger.warning("Column 'state' not found, creating with default value")
        df = df.withColumn('state', lit('Unknown'))
    else:
        df = df.fillna({'state': 'Unknown'})
    
    # Sanitize partition names
    df = df.withColumn("state", trim(col("state")))
    df = df.withColumn(
        "state",
        regexp_replace(
            regexp_replace(
                regexp_replace(col("state"), r'[/\\:*?"<>|]', '_'),
                r'[\x00-\x1f\x7f]', ''
            ),
            r'_+', '_'
        )
    )
    
    sanitize_udf = udf(sanitize_partition_name, StringType())
    df = df.withColumn("state", sanitize_udf(col("state")))
    df = df.withColumn("state", when((col("state") == "") | col("state").isNull(), "Unknown").otherwise(col("state")))
    
    validate_schema(df, expected_columns=['state'])
    
    logger.info(f"Saving Silver data to {silver_root} partitioned by state")
    temp_silver_path = f"/tmp/silver_{uuid.uuid4()}"
    
    try:
        df.write \
            .mode("overwrite") \
            .option("compression", PARQUET_COMPRESSION) \
            .partitionBy("state") \
            .parquet(temp_silver_path)
        
        if os.path.exists(silver_root):
            shutil.rmtree(silver_root)
        
        os.makedirs(os.path.dirname(silver_root), exist_ok=True)
        shutil.copytree(temp_silver_path, silver_root, dirs_exist_ok=True)
        shutil.rmtree(temp_silver_path)
        logger.info(f"Silver data saved to {silver_root}")
    except Exception as e:
        logger.error(f"Failed to write/move Silver data: {e}")
        if os.path.exists(temp_silver_path):
            shutil.rmtree(temp_silver_path)
        raise

    logger.info("Silver Layer processing complete.")

def process_silver_to_gold(silver_root, gold_file):
    """Reads Silver Parquet, aggregates breweries by type and location, saves to Gold."""
    spark = get_spark_session("SilverToGold")
    
    if not os.path.exists(silver_root):
        raise FileNotFoundError(f"Silver directory not found: {silver_root}")
    
    logger.info(f"Reading Silver data from {silver_root}")
    df = spark.read \
        .option("mergeSchema", "true") \
        .option("mergeSchema", "true") \
        .parquet(silver_root)
    
    record_count = df.count()
    logger.info(f"Read {record_count} records from Silver layer")
    
    if record_count == 0:
        raise ValueError("Silver layer contains no data")
    
    validate_schema(df, expected_columns=['brewery_type', 'state'])
    
    logger.info("Aggregating data...")
    df = df.withColumn("brewery_type", when(col("brewery_type").isNull(), "unknown").otherwise(col("brewery_type")))
    df = df.withColumn("state", when(col("state").isNull(), "Unknown").otherwise(col("state")))
    
    agg_df = df.groupBy("brewery_type", "state").count().withColumnRenamed("count", "brewery_count")
    agg_df = agg_df.filter(col("brewery_count") > 0).orderBy(col("brewery_count").desc())
    
    agg_count = agg_df.count()
    if agg_count == 0:
        raise ValueError("Aggregation produced no results")
    
    total_breweries = agg_df.agg({"brewery_count": "sum"}).collect()[0][0]
    logger.info(f"Aggregation complete: {agg_count} groups, {total_breweries} total breweries")
    
    if total_breweries <= 0:
        raise ValueError(f"Invalid aggregation: total brewery count is {total_breweries}")
    
    logger.info(f"Saving Gold data to {gold_file}")
    temp_gold_path = f"/tmp/gold_{uuid.uuid4()}"
    
    try:
        validate_schema(agg_df, expected_columns=['brewery_type', 'state', 'brewery_count'])
        agg_df.coalesce(1).write \
            .mode("overwrite") \
            .option("compression", PARQUET_COMPRESSION) \
            .parquet(temp_gold_path)
        
        if os.path.exists(gold_file):
            shutil.rmtree(gold_file) if os.path.isdir(gold_file) else os.remove(gold_file)
        
        os.makedirs(os.path.dirname(gold_file), exist_ok=True)
        shutil.copytree(temp_gold_path, gold_file, dirs_exist_ok=True)
        shutil.rmtree(temp_gold_path)
        logger.info(f"Gold data saved to {gold_file}")
    except Exception as e:
        logger.error(f"Failed to write/move Gold data: {e}")
        if os.path.exists(temp_gold_path):
            shutil.rmtree(temp_gold_path)
        raise

    logger.info("Gold Layer processing complete.")

if __name__ == "__main__":
    BRONZE = "datalake/bronze/breweries_raw.json"
    SILVER = "datalake/silver/breweries"
    GOLD = "datalake/gold/brewery_summary.parquet"
    
    if os.path.exists(BRONZE):
        process_bronze_to_silver(BRONZE, SILVER)
        process_silver_to_gold(SILVER, GOLD)
    else:
        print("Bronze file not found, skipping local test.")
