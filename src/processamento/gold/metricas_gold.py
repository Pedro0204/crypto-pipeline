"""Gold ETL: Silver Iceberg para Star Schema."""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, current_timestamp, lit, max as spark_max,
    min as spark_min, stddev, sum as spark_sum,
    percentile_approx, when,
)
from loguru import logger

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("crypto-gold-etl")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg_silver", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg_silver.type", "hadoop")
        .config("spark.sql.catalog.iceberg_silver.warehouse", "s3a://silver/warehouse")
        .config("spark.sql.catalog.iceberg_gold", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg_gold.type", "hadoop")
        .config("spark.sql.catalog.iceberg_gold.warehouse", "s3a://gold/warehouse")
        .getOrCreate()
    )


def build_fact_table(spark: SparkSession, execution_date: str):
    """fct_metricas_hora: agregações horárias por moeda."""
    logger.info("Construindo fct_metricas_hora")

    df = spark.read.format("iceberg").load("iceberg_silver.crypto.coins_markets_silver")
    df = df.filter(col("dt") == execution_date)

    fct = (
        df.groupBy("id", "symbol", "name", "dt", "hour")
        .agg(
            count("*").alias("total_registros"),
            avg("current_price").alias("preco_medio"),
            spark_min("current_price").alias("preco_min"),
            spark_max("current_price").alias("preco_max"),
            stddev("current_price").alias("preco_desvio"),
            avg("market_cap").alias("market_cap_medio"),
            avg("total_volume").alias("volume_medio"),
            avg("price_change_24h").alias("variacao_24h_media"),
            avg("price_change_percentage_24h").alias("variacao_pct_24h_media"),
            spark_max("high_24h").alias("high_24h"),
            spark_min("low_24h").alias("low_24h"),
        )
        .withColumn("spread_24h", col("high_24h") - col("low_24h"))
        .withColumn("volatilidade_relativa",
                    when(col("preco_medio") > 0,
                         col("preco_desvio") / col("preco_medio") * 100)
                    .otherwise(0))
        .withColumn("_processed_at", current_timestamp())
    )

    fct.writeTo("iceberg_gold.crypto.fct_metricas_hora") \
        .using("iceberg") \
        .partitionedBy("dt") \
        .createOrReplace()

    logger.info(f"fct_metricas_hora: {fct.count()} registros")


def build_dim_moedas(spark: SparkSession, execution_date: str):
    """dim_moedas: dimensão com snapshot diário de cada moeda."""
    logger.info("Construindo dim_moedas")

    df = spark.read.format("iceberg").load("iceberg_silver.crypto.coins_markets_silver")
    df = df.filter(col("dt") == execution_date)

    # Pegar o último registro de cada moeda no dia
    from pyspark.sql.window import Window
    w = Window.partitionBy("id").orderBy(col("ingestion_ts").desc())
    from pyspark.sql.functions import row_number

    dim = (
        df
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .select(
            "id", "symbol", "name", "market_cap_rank",
            "current_price", "market_cap", "total_volume",
            "high_24h", "low_24h",
            "price_change_24h", "price_change_percentage_24h",
            "dt",
        )
        .withColumn("_processed_at", current_timestamp())
        .drop("_rn")
    )

    dim.writeTo("iceberg_gold.crypto.dim_moedas") \
        .using("iceberg") \
        .partitionedBy("dt") \
        .createOrReplace()

    logger.info(f"dim_moedas: {dim.count()} registros")


def run(execution_date: str):
    spark = create_spark_session()
    build_fact_table(spark, execution_date)
    build_dim_moedas(spark, execution_date)
    logger.info("Gold ETL concluído")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Gold  Star Schema Cripto")
    parser.add_argument("--execution_date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()
    run(args.execution_date)
