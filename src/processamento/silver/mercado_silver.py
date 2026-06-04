"""Silver ETL: Bronze JSON para Iceberg, tipagem e dedup."""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, row_number, to_timestamp,
)
from pyspark.sql.window import Window
from loguru import logger

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("crypto-silver-etl")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://silver/warehouse")
        .getOrCreate()
    )


def run(execution_date: str):
    spark = create_spark_session()

    logger.info(f"Lendo Bronze para dt={execution_date}")
    df_raw = (
        spark.read
        .option("basePath", "s3a://bronze/coins_markets/")
        .json(f"s3a://bronze/coins_markets/dt={execution_date}/")
    )

    if df_raw.rdd.isEmpty():
        logger.warning("Sem dados no Bronze para essa data")
        spark.stop()
        return

    df = (
        df_raw
        .withColumn("current_price", col("current_price").cast("double"))
        .withColumn("market_cap", col("market_cap").cast("double"))
        .withColumn("total_volume", col("total_volume").cast("double"))
        .withColumn("price_change_24h", col("price_change_24h").cast("double"))
        .withColumn("price_change_percentage_24h", col("price_change_percentage_24h").cast("double"))
        .withColumn("high_24h", col("high_24h").cast("double"))
        .withColumn("low_24h", col("low_24h").cast("double"))
        .withColumn("market_cap_rank", col("market_cap_rank").cast("int"))
        .withColumn("ingestion_ts", to_timestamp("ingestion_ts"))
        .withColumn("_processed_at", current_timestamp())
    )

    # Deduplicação: manter registro mais recente por (id, dt, hour)
    window = Window.partitionBy("id", "dt", "hour").orderBy(col("ingestion_ts").desc())
    df_dedup = (
        df
        .withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    logger.info(f"Registros antes: {df.count()}, após dedup: {df_dedup.count()}")

    # Grava como tabela Iceberg particionada por dt
    df_dedup.writeTo("iceberg.crypto.coins_markets_silver") \
        .using("iceberg") \
        .partitionedBy("dt") \
        .createOrReplace()

    logger.info("Silver gravado com sucesso no Iceberg")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Silver  Mercado Cripto")
    parser.add_argument("--execution_date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()
    run(args.execution_date)
