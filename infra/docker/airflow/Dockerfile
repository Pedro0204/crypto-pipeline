FROM apache/airflow:2.10.5-python3.11

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.11.2 \
    pyspark==3.5.3

ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PATH=${SPARK_HOME}/bin:${PATH}
