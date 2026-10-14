import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    count,
    sum as _sum,
    avg,
    round as _round,
    explode,
    when
)
from delta import configure_spark_with_delta_pip


def get_spark_session(app_name="Gold-Aggregations"):
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


def run_gold_pipeline():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    silver_path = os.path.join(base_dir, "data", "lakehouse", "silver", "orders")
    gold_daily_path = os.path.join(base_dir, "data", "lakehouse", "gold", "daily_revenue_metrics")
    gold_cat_path = os.path.join(base_dir, "data", "lakehouse", "gold", "category_performance")

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[*] Reading Silver Delta records from: {silver_path}")
    silver_df = spark.read.format("delta").load(silver_path)

    # -------------------------------------------------------------
    # 1. Mart 1: Daily Revenue & Order Status Metrics
    # -------------------------------------------------------------
    daily_metrics_df = (
        silver_df.withColumn("order_date", to_date(col("event_time")))
        .groupBy("order_date")
        .agg(
            count("transaction_id").alias("total_orders"),
            _round(_sum(when(col("status") == "COMPLETED", col("total_amount")).otherwise(0)), 2).alias("completed_revenue"),
            count(when(col("status") == "COMPLETED", True)).alias("completed_count"),
            count(when(col("status") == "FAILED", True)).alias("failed_count"),
            count(when(col("status") == "CANCELLED", True)).alias("cancelled_count"),
            _round(avg("total_amount"), 2).alias("avg_order_value")
        )
        .orderBy(col("order_date").desc())
    )

    print("[*] Sample Daily Revenue Metrics:")
    daily_metrics_df.show(5, truncate=False)

    daily_metrics_df.write.format("delta").mode("overwrite").save(gold_daily_path)
    print(f"[+] Written daily revenue metrics to: {gold_daily_path}")

    # -------------------------------------------------------------
    # 2. Mart 2: Exploded Category Performance
    # -------------------------------------------------------------
    category_df = (
        silver_df.filter(col("status") == "COMPLETED")
        .select("transaction_id", explode("items").alias("item"))
        .select(
            col("transaction_id"),
            col("item.category").alias("category"),
            col("item.unit_price").alias("unit_price"),
            col("item.quantity").alias("quantity"),
            (col("item.unit_price") * col("item.quantity")).alias("line_total")
        )
        .groupBy("category")
        .agg(
            count("transaction_id").alias("item_orders"),
            _sum("quantity").alias("total_units_sold"),
            _round(_sum("line_total"), 2).alias("total_gross_revenue"),
            _round(avg("unit_price"), 2).alias("avg_selling_price")
        )
        .orderBy(col("total_gross_revenue").desc())
    )

    print("[*] Sample Category Performance:")
    category_df.show(truncate=False)

    category_df.write.format("delta").mode("overwrite").save(gold_cat_path)
    print(f"[+] Written category performance to: {gold_cat_path}")

    print("[SUCCESS] Gold layer aggregation mart created successfully.")
    spark.stop()


if __name__ == "__main__":
    run_gold_pipeline()