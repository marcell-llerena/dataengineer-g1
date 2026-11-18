FROM apache/airflow:3.3.2

# Provider de Amazon: S3Hook, AthenaHook, AthenaOperator, GlueJobOperator, S3KeySensor...
# La imagen oficial ya lo trae (junto con boto3, pandas y pyarrow); lo fijamos aquí para que la
# dependencia quede explícita. Se instala junto a "apache-airflow==<versión>" para que pip
# no actualice Airflow por accidente.
RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    "apache-airflow-providers-amazon==9.36.0"
