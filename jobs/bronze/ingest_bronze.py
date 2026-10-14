import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, col
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
)
from delta import configure_spark_with_delta_pip


def get_spark_session(app_name="Bronze-Ingestion"):
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def get_bronze_schema():
    """Explicit schema definition with quarantine field for malformed payloads."""
    customer_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
    ])

    item_schema = StructType([
        StructField("item_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
    ])

    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("status", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("customer", customer_schema, True),
        StructField("items", ArrayType(item_schema), True),
        StructField("_corrupt_record", StringType(), True),
    ])


def run_bronze_ingestion():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    landing_path = os.path.join(base_dir, "data", "landing")
    bronze_table_path = os.path.join(base_dir, "data", "lakehouse", "bronze", "transactions")
    quarantine_table_path = os.path.join(base_dir, "data", "lakehouse", "bronze", "quarantine")

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    schema = get_bronze_schema()

    print(f"[*] Reading raw JSON payloads from: {landing_path}")
    raw_df = (
        spark.read.schema(schema)
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("mode", "PERMISSIVE")
        .json(landing_path)
    )

    # Inject lineage metadata and cache to allow dual routing (clean vs quarantine)
    enriched_df = (
        raw_df.withColumn("_ingest_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .cache()
    )

    # Route corrupt vs clean
    corrupt_df = enriched_df.filter(col("_corrupt_record").isNotNull())
    clean_bronze_df = enriched_df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

    clean_count = clean_bronze_df.count()
    corrupt_count = corrupt_df.count()

    print(f"[+] Clean records to Bronze Delta: {clean_count}")
    print(f"[!] Corrupt records to Quarantine: {corrupt_count}")

    # Write clean records to Bronze Delta
    clean_bronze_df.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).save(bronze_table_path)

    # Write quarantined rows
    if corrupt_count > 0:
        corrupt_df.write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).save(quarantine_table_path)

    # Free cache memory
    enriched_df.unpersist()

    print("[SUCCESS] Bronze layer ingestion complete.")
    spark.stop()


if __name__ == "__main__":
    run_bronze_ingestion()