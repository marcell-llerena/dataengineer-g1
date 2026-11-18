"""
DAG ventas_diarias_aws: Airflow local orquestando servicios de AWS.

Cada ejecución procesa UN día ({{ ds }}) de las ventas que dejó el ETL local en data/staging/:

    verificar_recursos ─┬─> preparar_catalogo ─┐
                        ├─> subir_ventas ──────┴─> registrar_particion_ventas ─┐
                        └─> subir_script ──────────────────────────────────────┴─> ejecutar_glue_job
                                                                                         │
            validar_resultado <── registrar_particion_diarias <── esperar_salida <──────┘

Es lo mismo que hicimos a mano en los notebooks 01 a 04, ahora automático y para cada día:

    notebook 01  subir el Parquet particionado a S3            -> subir_ventas          (S3Hook)
    notebook 02  registrar la partición en el catálogo         -> registrar_particion_* (AthenaOperator)
    notebook 03  consultar con Athena                          -> validar_resultado     (AthenaHook)
    notebook 04  subir el script y ejecutar el Glue Job        -> subir_script + ejecutar_glue_job (GlueJobOperator)

Hook     = tu propio código Python con las credenciales de la Connection (dentro de un @task).
Operator = una tarea ya armada: solo le pasas parámetros.
Sensor   = un operator que espera a que algo ocurra.
"""

import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import chain, dag, task
from airflow.sdk.exceptions import AirflowFailException

# ------------------------------------------------------------------ CONFIGURACIÓN
AWS_CONN = "aws_curso"                       # Connection definida en .env (AIRFLOW_CONN_AWS_CURSO)
BUCKET = os.environ.get("AWS_BUCKET", "")    # definido en .env
DATABASE = "ventas_db"
ROL_GLUE = "AWSGlueServiceRole-curso"        # lo crea el notebook 02 (ejercicio 1)
GLUE_JOB = "ventas-diarias"                  # el mismo job del notebook 04
SCRIPT_KEY = "scripts/ventas_diarias.py"
ATHENA_RESULTADOS = f"s3://{BUCKET}/athena-results/"

DATA = Path("/opt/airflow/data")             # fundamentos-aws/data montada en el contenedor
GLUE_JOBS = Path("/opt/airflow/glue_jobs")   # fundamentos-aws/glue_jobs montada en el contenedor

# Fecha lógica de cada ejecución (ej. "2026-10-22"). Airflow la renderiza en los operators
# y también en los argumentos de los @task
FECHA = "{{ ds }}"

# Partición del día a partir de la fecha lógica: 2026-10-22 -> year=2026/month=10/day=22
PARTICION = "year={{ ds[:4] }}/month={{ ds[5:7] }}/day={{ ds[8:] }}"
SPEC_PARTICION = "year='{{ ds[:4] }}', month='{{ ds[5:7] }}', day='{{ ds[8:] }}'"

# Si el crawler (notebooks 02 y 04) ya creó las tablas, IF NOT EXISTS no hace nada.
# Las sentencias DDL no se cobran en Athena.
DDL_CATALOGO = [
    f"CREATE DATABASE IF NOT EXISTS {DATABASE}",
    f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.ventas (
        producto_id bigint, nombre string, categoria string, precio_unitario double,
        cantidad bigint, pedido_id string, fecha string, cliente_id bigint,
        canal string, monto double
    )
    PARTITIONED BY (year string, month string, day string)
    STORED AS PARQUET
    LOCATION 's3://{BUCKET}/processed/ventas/'
    """,
    f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.ventas_diarias (
        canal string, categoria string, pedidos bigint, unidades bigint,
        monto_total double, ticket_promedio double
    )
    PARTITIONED BY (year string, month string, day string)
    STORED AS PARQUET
    LOCATION 's3://{BUCKET}/curated/ventas_diarias/'
    """,
]


def ejecutar_en_athena(athena: AthenaHook, sql: str, database: str | None = None) -> str:
    """Envía una consulta, espera a que termine y devuelve su id. Falla si no termina en SUCCEEDED."""
    # Athena exige base de datos o catálogo en el contexto. Para sentencias sin base
    # (CREATE DATABASE) indicamos el catálogo por defecto: el Glue Data Catalog
    contexto = {"Database": database} if database else {"Catalog": "AwsDataCatalog"}
    qid = athena.run_query(
        sql,
        query_context=contexto,
        result_configuration={"OutputLocation": ATHENA_RESULTADOS},
    )
    estado = athena.poll_query_status(qid, sleep_time=2)
    if estado != "SUCCEEDED":
        raise RuntimeError(f"Athena: {estado} · {athena.get_state_change_reason(qid)}")
    return qid


def comparar_con_reporte(reporte: pd.DataFrame, nube: pd.DataFrame) -> pd.DataFrame:
    """Une el reporte local del ETL con el resultado del Glue Job y marca qué filas coinciden."""
    comparacion = reporte.merge(nube, on=["canal", "categoria"], how="outer", suffixes=("_local", "_nube"))
    comparacion["ok"] = (
        comparacion.notna().all(axis=1)
        & (comparacion["pedidos_local"] == comparacion["pedidos_nube"])
        & (comparacion["unidades_local"] == comparacion["unidades_nube"])
        & ((comparacion["monto_total_local"] - comparacion["monto_total_nube"]).abs() <= 0.01)
    )
    return comparacion


@dag(
    dag_id="ventas_diarias_aws",
    schedule="@daily",
    start_date=datetime(2026, 10, 20),
    end_date=datetime(2026, 10, 27),   # solo hay datos de estos 8 días
    catchup=True,
    max_active_runs=1,                 # un día a la vez: el Glue Job admite 1 ejecución concurrente
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    doc_md=__doc__,
    tags=["aws", "s3", "glue", "athena"],
)
def ventas_diarias_aws():

    # --------------------------------------------------------------- HOOKS
    @task
    def verificar_recursos():
        """AwsBaseHook: cualquier cliente de boto3, con las credenciales de la Connection."""
        # .conn es el cliente de boto3. boto3 crea sus métodos al vuelo, así que el editor
        # no los conoce: lo anotamos como Any
        sts: Any = AwsBaseHook(aws_conn_id=AWS_CONN, client_type="sts").conn
        print("Trabajando como:", sts.get_caller_identity()["Arn"])   # el "hola mundo" del notebook 01

        if not BUCKET or BUCKET == "de-tecsup-tualias":
            raise AirflowFailException("Define AWS_BUCKET en el archivo .env con el nombre de tu bucket")
        if not S3Hook(aws_conn_id=AWS_CONN).check_for_bucket(BUCKET):
            raise AirflowFailException(f"No existe el bucket {BUCKET} (o no es tuyo): ejecuta el notebook 01")

        iam: Any = AwsBaseHook(aws_conn_id=AWS_CONN, client_type="iam").conn
        try:
            iam.get_role(RoleName=ROL_GLUE)
        except iam.exceptions.NoSuchEntityException:
            raise AirflowFailException(f"No existe el rol {ROL_GLUE}: ejecuta el notebook 02 (ejercicio 1)")
        print(f"OK: bucket {BUCKET} y rol {ROL_GLUE}")

    @task
    def preparar_catalogo():
        """AthenaHook: crea la base y las tablas si no existen."""
        athena = AthenaHook(aws_conn_id=AWS_CONN)
        for sql in DDL_CATALOGO:
            ejecutar_en_athena(athena, sql)
            print("OK:", " ".join(sql.split())[:80])

    @task
    def subir_ventas(fecha: str) -> str:
        """S3Hook: el mismo upload del notebook 01, pero para el día de esta ejecución."""
        origen = DATA / "staging" / fecha / "ventas.csv"
        if not origen.exists():
            raise AirflowFailException(f"No hay datos locales para {fecha}: falta {origen}")

        y, m, d = fecha.split("-")
        key = f"processed/ventas/year={y}/month={m}/day={d}/ventas.parquet"
        with tempfile.TemporaryDirectory() as tmp:
            parquet = Path(tmp) / "ventas.parquet"
            pd.read_csv(origen).to_parquet(parquet, index=False)
            S3Hook(aws_conn_id=AWS_CONN).load_file(parquet, key=key, bucket_name=BUCKET, replace=True)

        print(f"Subido: s3://{BUCKET}/{key}")
        return key

    @task
    def subir_script() -> str:
        """S3Hook: Glue siempre lee el script desde S3, nunca desde tu máquina."""
        S3Hook(aws_conn_id=AWS_CONN).load_file(
            GLUE_JOBS / "ventas_diarias.py", key=SCRIPT_KEY, bucket_name=BUCKET, replace=True
        )
        print(f"Subido: s3://{BUCKET}/{SCRIPT_KEY}")
        return SCRIPT_KEY

    # ----------------------------------------------------------- OPERATORS
    # Sin la partición registrada, ni Athena ni el Glue Job (from_catalog) ven el día nuevo
    registrar_particion_ventas = AthenaOperator(
        task_id="registrar_particion_ventas",
        aws_conn_id=AWS_CONN,
        database=DATABASE,
        query=(
            f"ALTER TABLE ventas ADD IF NOT EXISTS PARTITION ({SPEC_PARTICION}) "
            f"LOCATION 's3://{BUCKET}/processed/ventas/{PARTICION}/'"
        ),
        output_location=ATHENA_RESULTADOS,
        sleep_time=5,
    )

    # Si el job del notebook 04 existe, lo usa tal cual. Si no, lo crea con esta configuración.
    ejecutar_glue_job = GlueJobOperator(
        task_id="ejecutar_glue_job",
        aws_conn_id=AWS_CONN,
        job_name=GLUE_JOB,
        job_desc="Agrega una partición diaria de ventas por canal y categoría hacia curated/",
        script_location=f"s3://{BUCKET}/{SCRIPT_KEY}",
        iam_role_name=ROL_GLUE,
        concurrent_run_limit=1,
        create_job_kwargs={
            "GlueVersion": "5.0",
            "WorkerType": "G.1X",
            "NumberOfWorkers": 2,
            "Timeout": 10,
            "DefaultArguments": {"--BUCKET": BUCKET, "--job-language": "python"},
        },
        script_args={"--FECHA": FECHA},          # cambia en cada ejecución
        wait_for_completion=True,
        job_poll_interval=15,
        retries=0,                             # no relanzar solo: cada ejecución cuesta
    )

    # Un job SUCCEEDED no garantiza que escribió datos: lo comprobamos en S3
    esperar_salida = S3KeySensor(
        task_id="esperar_salida",
        aws_conn_id=AWS_CONN,
        bucket_name=BUCKET,
        bucket_key=f"curated/ventas_diarias/{PARTICION}/*.parquet",
        wildcard_match=True,
        poke_interval=30,
        timeout=10 * 60,
        mode="reschedule",   # libera el worker entre comprobaciones
    )

    registrar_particion_diarias = AthenaOperator(
        task_id="registrar_particion_diarias",
        aws_conn_id=AWS_CONN,
        database=DATABASE,
        query=(
            f"ALTER TABLE ventas_diarias ADD IF NOT EXISTS PARTITION ({SPEC_PARTICION}) "
            f"LOCATION 's3://{BUCKET}/curated/ventas_diarias/{PARTICION}/'"
        ),
        output_location=ATHENA_RESULTADOS,
        sleep_time=5,
    )

    @task
    def validar_resultado(fecha: str):
        """AthenaHook: lo que calculó Glue en la nube debe coincidir con el reporte del ETL local."""
        y, m, d = fecha.split("-")
        athena = AthenaHook(aws_conn_id=AWS_CONN)
        qid = ejecutar_en_athena(
            athena,
            f"""
            SELECT canal, categoria, pedidos, unidades, monto_total
            FROM ventas_diarias
            WHERE year = '{y}' AND month = '{m}' AND day = '{d}'
            """,
            database=DATABASE,
        )

        resultado = athena.get_query_results(qid)
        if resultado is None:
            raise AirflowFailException(f"Athena no devolvió resultados para {fecha}")
        filas = resultado["ResultSet"]["Rows"]
        columnas = [c["VarCharValue"] for c in filas[0]["Data"]]        # la 1.ª fila son los nombres
        nube = pd.DataFrame([[c.get("VarCharValue") for c in f["Data"]] for f in filas[1:]], columns=columnas)
        for col in ["pedidos", "unidades", "monto_total"]:
            nube[col] = pd.to_numeric(nube[col])                          # Athena devuelve texto

        reporte = pd.read_csv(DATA / "reportes" / f"reporte_{fecha}.csv")
        comparacion = comparar_con_reporte(reporte, nube)
        print(comparacion.to_string(index=False))

        if not bool(comparacion["ok"].all()):
            raise AirflowFailException(f"El resultado de Glue no coincide con el reporte local en {fecha}")
        print(f"OK: Glue y el ETL local coinciden en las {len(comparacion)} filas de {fecha}")

    # ---------------------------------------------------------- DEPENDENCIAS
    verificacion = verificar_recursos()
    catalogo = preparar_catalogo()
    ventas = subir_ventas(FECHA)
    script = subir_script()

    # chain(a, b) equivale a a >> b; con una lista, conecta cada elemento
    chain(verificacion, [catalogo, ventas, script])
    chain([catalogo, ventas], registrar_particion_ventas)
    chain([registrar_particion_ventas, script], ejecutar_glue_job)
    chain(ejecutar_glue_job, esperar_salida, registrar_particion_diarias, validar_resultado(FECHA))


ventas_diarias_aws()
