from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import requests
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, date_format, lit, udf,
    from_json, explode, struct,
)
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, StringType,
    StructField, StructType, TimestampType,
)

COINGECKO_URL = os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3")
PAGE_SIZE = int(os.getenv("COINGECKO_PAGE_SIZE", "250"))
PAGES = int(os.getenv("COINGECKO_PAGES_PER_CYCLE", "5"))
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BRONZE_BUCKET = "bronze"

COIN_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("symbol", StringType()),
    StructField("name", StringType()),
    StructField("current_price", DoubleType()),
    StructField("market_cap", DoubleType()),
    StructField("total_volume", DoubleType()),
    StructField("price_change_24h", DoubleType()),
    StructField("price_change_percentage_24h", DoubleType()),
    StructField("market_cap_rank", IntegerType()),
    StructField("high_24h", DoubleType()),
    StructField("low_24h", DoubleType()),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("crypto-bronze-streaming")
        .master(os.getenv("SPARK_MASTER", "local[1]"))
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", f"s3a://{BRONZE_BUCKET}/warehouse")
        .getOrCreate()
    )


def fetch_all_pages() -> list[dict]:
    """Coleta múltiplas páginas da API CoinGecko."""
    all_coins = []
    for page in range(1, PAGES + 1):
        try:
            resp = requests.get(
                f"{COINGECKO_URL}/coins/markets",
                params={
                    "vs_currency": "usd",
                    "order": "market_cap_desc",
                    "per_page": PAGE_SIZE,
                    "page": page,
                    "sparkline": "false",
                },
                timeout=30,
            )
            resp.raise_for_status()
            all_coins.extend(resp.json())
        except requests.RequestException as exc:
            print(f"Erro página {page}: {exc}")
    return all_coins


def process_batch(batch_df: DataFrame, batch_id: int):
    """foreachBatch: chama CoinGecko e grava JSON no MinIO Bronze."""
    spark = batch_df.sparkSession
    now = datetime.now(timezone.utc)

    coins = fetch_all_pages()
    if not coins:
        print(f"Batch {batch_id}: sem dados da API")
        return

    records = []
    for c in coins:
        records.append({
            "id": c.get("id"),
            "symbol": c.get("symbol"),
            "name": c.get("name"),
            "current_price": float(c.get("current_price") or 0),
            "market_cap": float(c.get("market_cap") or 0),
            "total_volume": float(c.get("total_volume") or 0),
            "price_change_24h": float(c.get("price_change_24h") or 0),
            "price_change_percentage_24h": float(c.get("price_change_percentage_24h") or 0),
            "market_cap_rank": c.get("market_cap_rank"),
            "high_24h": float(c.get("high_24h") or 0),
            "low_24h": float(c.get("low_24h") or 0),
        })

    df = spark.createDataFrame(records, schema=COIN_SCHEMA)

    df = (
        df
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("dt", lit(now.strftime("%Y-%m-%d")))
        .withColumn("hour", lit(now.strftime("%H")))
    )

    # Bronze: JSON particionado por moeda/data-hora
    (
        df.write
        .mode("append")
        .partitionBy("dt", "hour")
        .json(f"s3a://{BRONZE_BUCKET}/coins_markets/")
    )

    print(f"Batch {batch_id}: {len(records)} moedas gravadas no Bronze")


def main():
    spark = create_spark_session()

    # Rate source gera 1 linha a cada 30s como trigger
    rate_stream = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", 1)
        .load()
    )

    query = (
        rate_stream.writeStream
        .foreachBatch(process_batch)
        .trigger(processingTime="30 seconds")
        .start()
    )

    print("Spark Streaming iniciado  coletando CoinGecko a cada 30s")
    query.awaitTermination()


if __name__ == "__main__":
    main()
