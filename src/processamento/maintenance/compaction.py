"""Compactação diária das tabelas Iceberg (Silver + Gold)."""

import os

from pyspark.sql import SparkSession
from loguru import logger

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

TABLES = [
    ("s3a://silver/warehouse", "iceberg_silver", "crypto.coins_markets_silver"),
    ("s3a://gold/warehouse", "iceberg_gold", "crypto.fct_metricas_hora"),
    ("s3a://gold/warehouse", "iceberg_gold", "crypto.dim_moedas"),
]


def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("crypto-compaction")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    )
    for warehouse_path, catalog_name, _ in TABLES:
        builder = builder.config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        builder = builder.config(f"spark.sql.catalog.{catalog_name}.type", "hadoop")
        builder = builder.config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path)

    return builder.getOrCreate()


def run():
    spark = create_spark_session()

    for _, catalog_name, table_name in TABLES:
        full_name = f"{catalog_name}.{table_name}"
        try:
            logger.info(f"Compactando {full_name}")
            spark.sql(f"CALL {catalog_name}.system.rewrite_data_files(table => '{table_name}')")
            logger.info(f"Compactação concluída: {full_name}")
        except Exception as exc:
            logger.warning(f"Erro ao compactar {full_name}: {exc}")

    spark.stop()
    logger.info("Compactação diária finalizada")


if __name__ == "__main__":
    run()
