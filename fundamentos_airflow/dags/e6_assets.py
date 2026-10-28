import random
from datetime import datetime
from pathlib import Path

from airflow.sdk import Asset, dag, task
import pandas as pd


RUTA = "/opt/airflow/dags/data/clima.csv"
CLIMA = Asset(f"file://{RUTA}")


@dag(
    schedule="@daily",
    start_date=datetime(2026, 10, 23),
    catchup=True,
    tags=["ejercicio e6", "productor"],
)
def e6_productor():

    @task
    def listar_ciudades() -> list[str]:
        return ["lima", "quito", "bogota", "santiago"]

    @task
    def medir(ciudad: str, ds=None) -> dict[str, float | str]:
        temp = random.Random(f"{ciudad}-{ds}").uniform(8, 30)
        return {
            "ciudad": ciudad,
            "temp": round(temp, 1)
        }

    @task(outlets=[CLIMA])
    def guardar(medidas: list[dict[str, str | float]]):
        Path(RUTA).parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(list(medidas), columns=["ciudad", "temp"])
        df.to_csv(RUTA, index=False)

    guardar(medir.expand(ciudad=listar_ciudades()))


@dag(
    schedule=[CLIMA],
    start_date=datetime(2026, 10, 23),
    catchup=True,
    tags=["ejercicio e6", "consumidor"],
)
def e6_consumidor():
    @task
    def informe():
        df = pd.read_csv(RUTA)
        print(f"data: {df}")

    informe()


e6_productor()
e6_consumidor()
