"""Glue Job ventas-diarias.

Lee UNA partición (un día) de ventas_db.ventas desde el Data Catalog, la agrega
por canal y categoría, y escribe el resultado en la zona curated como Parquet
particionado por year/month/day.

Argumentos del job:
  --BUCKET  bucket del data lake (fijo: va en DefaultArguments del job)
  --FECHA   día a procesar, AAAA-MM-DD (cambia en cada ejecución)

Este archivo se ejecuta dentro de AWS Glue (Glue 5.0: Spark 3.5, Python 3.11).
Las librerías awsglue solo existen ahí: no lo ejecutes con el Python local.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# (1) Argumentos: Glue los pasa como --JOB_NAME, --BUCKET y --FECHA
args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "FECHA"])

# (2) Contexto: GlueContext envuelve Spark y da acceso al Data Catalog
glue = GlueContext(SparkContext.getOrCreate())
spark = glue.spark_session
# overwrite solo reemplaza las particiones que se escriben, no toda la carpeta
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
job = Job(glue)
job.init(args["JOB_NAME"], args)

# (3) Lectura: solo la partición del día. push_down_predicate filtra ANTES de leer S3
y, m, d = args["FECHA"].split("-")
dyf = glue.create_dynamic_frame.from_catalog(
    database="ventas_db",
    table_name="ventas",
    push_down_predicate=f"year='{y}' and month='{m}' and day='{d}'",
)

# Transformación: PySpark puro sobre el DataFrame
diario = (
    dyf.toDF()
    .groupBy("year", "month", "day", "canal", "categoria")
    .agg(
        F.countDistinct("pedido_id").alias("pedidos"),
        F.sum("cantidad").alias("unidades"),
        F.round(F.sum("monto"), 2).alias("monto_total"),
    )
    .withColumn("ticket_promedio", F.round(F.col("monto_total") / F.col("pedidos"), 2))
)

print(f"Filas agregadas para {args['FECHA']}: {diario.count()}")

# (4) Escritura: Parquet particionado en la zona curated
(
    diario.write.mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(f"s3://{args['BUCKET']}/curated/ventas_diarias/")
)

# (5) Commit: cierra el job (y guardaría los bookmarks si estuvieran activos)
job.commit()
