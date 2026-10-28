from datetime import datetime
from pathlib import Path
import shutil

from airflow.sdk import dag, task
from airflow.providers.standard.sensors.filesystem import FSHook
from airflow.providers.standard.sensors.filesystem import FileSensor

import pandas as pd


ENTRADA = "entrada"
PROCESADOS = "procesados"
PATRON = "ventas_*.csv"


@dag(
    schedule=None,
    start_date=datetime(2026, 10, 23),
    catchup=False,
    max_active_runs=1,
    tags=["ejercicio e6", "productor"],
)
def e7_sensor_archivo():
    esperar_archivo = FileSensor(
        task_id="esperar_archivo",
        fs_conn_id="fs_default",
        filepath=f"{ENTRADA}/{PATRON}",
        poke_interval=10,
        timeout=60*100,
        mode="reschedule"
    )

    @task
    def procesar():
        base = Path(FSHook("fs_default").get_path())
        archivos = list((base / ENTRADA).glob(PATRON))
        for archivo in archivos:
            df = pd.read_csv(archivo)
            unidades = int(df["unidades"].sum())
            importe = (df["unidades"] * df["precio"]).sum()
            print(
                f"{archivo.name}: {len(df)} filas | {unidades} unidades | import {importe}"
            )

        return [archivo.name for archivo in archivos]

    @task
    def archivar(nombres: list[str]):
        base = Path(FSHook("fs_default").get_path())
        destino = base / PROCESADOS
        destino.mkdir(parents=True, exist_ok=True)

        marca_tp = datetime.now().strftime("%Y%m%d_%H%M%S")
        print(nombres)
        for nombre in nombres:
            shutil.move(base / ENTRADA / nombre, destino / f"{marca_tp}_{nombre}")
            print(f"Archivado: {nombre} -> {PROCESADOS}/{marca_tp}_{nombre}")

    procesado = procesar()
    esperar_archivo >> procesado
    archivar(procesado)


e7_sensor_archivo()
