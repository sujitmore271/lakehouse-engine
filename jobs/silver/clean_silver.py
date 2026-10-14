import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    sha2,
    concat,
    lit,
    coalesce,
    to_timestamp,
    row_number,
    current_timestamp,
    explode,
    when
)
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


SALT = "LakehouseSaltSecret_2026"


def get_spark_session(app_name="Silver-Transformation"):
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


def run_silver_pipeline():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    bronze_path = os.path.join(base_dir, "data", "lakehouse", "bronze", "transactions")
    silver_path = os.path.join(base_dir, "data", "lakehouse", "silver", "orders")

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[*] Reading Bronze Delta records from: {bronze_path}")
    bronze_df = spark.read.format("delta").load(bronze_path)

    # 1. Structural Flattening & PII Masking
    silver_stg = (
        bronze_df
        .withColumn("event_time", to_timestamp(col("event_timestamp")))
        # SHA-256 salted hashing for PII
        .withColumn(
            "masked_email",
            when(col("customer.email").isNotNull(), sha2(concat(col("customer.email"), lit(SALT)), 256))
            .otherwise(lit(None))
        )
        .withColumn(
            "masked_phone",
            when(col("customer.phone").isNotNull(), sha2(concat(col("customer.phone"), lit(SALT)), 256))
            .otherwise(lit(None))
        )
        .withColumn("customer_id", col("customer.customer_id"))
        .withColumn("customer_city", col("customer.city"))
        .withColumn("customer_country", col("customer.country"))
        # Data Hygiene: Clean amounts (convert anomalous negatives to absolute)
        .withColumn(
            "clean_total_amount",
            when(col("total_amount") < 0, -col("total_amount")).otherwise(col("total_amount"))
        )
        .withColumn("_silver_processed_timestamp", current_timestamp())
    )

    # 2. CDC Deduplication: Keep the latest state per transaction_id
    window_spec = Window.partitionBy("transaction_id").orderBy(col("event_time").desc())
    deduped_df = (
        silver_stg
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num", "customer", "event_timestamp", "total_amount")
        .withColumnRenamed("clean_total_amount", "total_amount")
    )

    print(f"[+] Records prepared after CDC deduplication: {deduped_df.count()}")

    # 3. Delta Lake ACID MERGE INTO (Upsert logic)
    if not DeltaTable.isDeltaTable(spark, silver_path):
        print(f"[*] Initializing Silver Delta table at: {silver_path}")
        deduped_df.write.format("delta").mode("overwrite").partitionBy("status").save(silver_path)
    else:
        print(f"[*] Merging updates into existing Silver table at: {silver_path}")
        target_table = DeltaTable.forPath(spark, silver_path)
        (
            target_table.alias("target")
            .merge(
                deduped_df.alias("source"),
                "target.transaction_id = source.transaction_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    print("[SUCCESS] Silver layer pipeline executed successfully.")
    spark.stop()


if __name__ == "__main__":
    run_silver_pipeline()