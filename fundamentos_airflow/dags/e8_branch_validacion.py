from datetime import datetime
from pathlib import Path
import shutil

from airflow.sdk import dag, task
from airflow.providers.standard.sensors.filesystem import FSHook
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.python import BranchPythonOperator

import pandas as pd


RECEPCION = "recepcion"
PROCESADOS = "procesados"
RECHAZADOS = "rechazados"
PATRON = "ventas_*.csv"
COLUMNAS = {"producto", "unidades", "precio"}


def revisar(ruta: Path) -> list[str]:
    df = pd.read_csv(ruta, dtype=str)

    columnas_faltantes = COLUMNAS - set(df.columns)

    if columnas_faltantes:
        return [f"faltan columnas: {', '.join(columnas_faltantes)}"]

    if df.empty:
        return ["el archivo no tiene filas"]

    return []


def elegir_rama(nombre: str) -> str:
    ruta = Path(FSHook("fs_default").get_path()) / RECEPCION / nombre
    errores = revisar(ruta)
    if errores:
        return "rechazar"
    return "procesar"


# https://crontab.cronhub.io/
# Configurar scheduler u horario de disparo
@dag(
    schedule="*/5 10 * * *",
    start_date=datetime(2026, 10, 23),
    catchup=False,
    max_active_runs=1,
    tags=["ejercicio e8", "branch validator"],
)
def e8_branch_validacion():
    esperar_archivo = FileSensor(
        task_id="esperar_archivo",
        fs_conn_id="fs_default",
        filepath=f"{RECEPCION}/{PATRON}",
        poke_interval=10,
        timeout=60*100,
        mode="reschedule"
    )

    @task
    def tomar_archivo():
        # /opt/airflow/dags/data
        carpeta = Path(FSHook("fs_default").get_path()) / RECEPCION
        archivo = list(carpeta.glob(PATRON))[0]
        print(f"Archivo validar: {archivo.name}")
        return archivo.name

    archivo = tomar_archivo()

    validar = BranchPythonOperator(
        task_id="validar",
        python_callable=elegir_rama,
        op_kwargs={"nombre": archivo}
    )

    @task
    def procesar(nombre: str) -> str:
        base = Path(FSHook("fs_default").get_path())
        df = pd.read_csv(base / RECEPCION / nombre)
        unidades = int(df["unidades"].sum())
        importe = (df["unidades"] * df["precio"]).sum()

        destino = base / PROCESADOS
        destino.mkdir(parents=True, exist_ok=True)

        marca_tp = datetime.now().strftime("%Y%m%d_%H%M%S")
        shutil.move(base / RECEPCION / nombre, destino / f"{marca_tp}_{nombre}")

        print(
            f"{nombre}: {len(df)} filas | {unidades} unidades | import {importe}"
        )
        return f"{nombre}: {len(df)} filas | {unidades} unidades | import {importe}"

    @task
    def rechazar(nombre: str) -> list[str]:
        base = Path(FSHook("fs_default").get_path())
        errores = revisar(base / RECEPCION / nombre)
        print(f"errores: {errores}")

        destino = base / RECHAZADOS
        destino.mkdir(parents=True, exist_ok=True)

        marca_tp = datetime.now().strftime("%Y%m%d_%H%M%S")
        shutil.move(base / RECEPCION / nombre, destino / f"{marca_tp}_{nombre}")

        return errores

    @task(trigger_rule="none_failed_min_one_success")
    def notificar(ti=None):
        resultado = ti.xcom_pull(task_ids="procesar") or ti.xcom_pull(task_ids="rechazar")
        print(f"resultado={resultado}")


    esperar_archivo >> archivo
    validar >> [procesar(archivo), rechazar(archivo)] >> notificar()


e8_branch_validacion()
