"""Airflow DAG  Orquestração do pipeline de criptoativos.

Fluxo diário:
  1. Ingestão Bronze (Spark Streaming roda contínuo, mas batch de segurança)
  2. Silver ETL (dedup + Iceberg)
  3. Gold ETL (Star Schema)
  4. Compactação diária
  5. Vacuum semanal (apenas domingos)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "crypto-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def should_vacuum(**context):
    if context["execution_date"].weekday() == 6:
        return "vacuum"
    return "skip_vacuum"


SPARK_CONN = "spark_default"
SPARK_CONF = {
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "{{ var.value.MINIO_ROOT_USER }}",
    "spark.hadoop.fs.s3a.secret.key": "{{ var.value.MINIO_ROOT_PASSWORD }}",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
}


with DAG(
    dag_id="crypto_pipeline",
    default_args=default_args,
    description="Pipeline Bronze → Silver → Gold para criptoativos",
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["crypto", "iceberg", "medallion"],
) as dag:

    silver_etl = SparkSubmitOperator(
        task_id="silver_etl",
        application="/jobs/processamento/silver/mercado_silver.py",
        application_args=["--execution_date", "{{ ds }}"],
        conn_id=SPARK_CONN,
        conf=SPARK_CONF,
    )

    gold_etl = SparkSubmitOperator(
        task_id="gold_etl",
        application="/jobs/processamento/gold/metricas_gold.py",
        application_args=["--execution_date", "{{ ds }}"],
        conn_id=SPARK_CONN,
        conf=SPARK_CONF,
    )

    compaction = SparkSubmitOperator(
        task_id="compaction",
        application="/jobs/processamento/maintenance/compaction.py",
        conn_id=SPARK_CONN,
        conf=SPARK_CONF,
    )

    check_vacuum = BranchPythonOperator(
        task_id="check_vacuum_day",
        python_callable=should_vacuum,
    )

    vacuum = SparkSubmitOperator(
        task_id="vacuum",
        application="/jobs/processamento/maintenance/vacuum.py",
        conn_id=SPARK_CONN,
        conf=SPARK_CONF,
    )

    skip_vacuum = EmptyOperator(task_id="skip_vacuum")

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    silver_etl >> gold_etl >> compaction >> check_vacuum >> [vacuum, skip_vacuum] >> end
