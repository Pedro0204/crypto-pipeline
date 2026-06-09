"""Vacuum semanal: expire snapshots + remove órfãos (7d)."""

import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from loguru import logger

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
RETENTION_DAYS = int(os.getenv("VACUUM_RETENTION_DAYS", "7"))

TABLES = [
    ("s3a://silver/warehouse", "iceberg_silver", "crypto.coins_markets_silver"),
    ("s3a://gold/warehouse", "iceberg_gold", "crypto.fct_metricas_hora"),
    ("s3a://gold/warehouse", "iceberg_gold", "crypto.dim_moedas"),
]


def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("crypto-vacuum")
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
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    cutoff_ts = int(cutoff.timestamp() * 1000)

    for _, catalog_name, table_name in TABLES:
        full_name = f"{catalog_name}.{table_name}"
        try:
            logger.info(f"Expirando snapshots > {RETENTION_DAYS}d em {full_name}")
            spark.sql(
                f"CALL {catalog_name}.system.expire_snapshots("
                f"table => '{table_name}', "
                f"older_than => TIMESTAMP '{cutoff.strftime('%Y-%m-%d %H:%M:%S')}')"
            )

            logger.info(f"Removendo arquivos órfãos de {full_name}")
            spark.sql(
                f"CALL {catalog_name}.system.remove_orphan_files("
                f"table => '{table_name}')"
            )

            logger.info(f"Vacuum concluído: {full_name}")
        except Exception as exc:
            logger.warning(f"Erro no vacuum de {full_name}: {exc}")

    spark.stop()
    logger.info("Vacuum semanal finalizado")


if __name__ == "__main__":
    run()
