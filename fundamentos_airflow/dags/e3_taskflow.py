from datetime import datetime

from airflow.sdk import dag, task


@dag(
    schedule=None,
    start_date=datetime(2026, 10, 23),
    catchup=False,
    tags=["ejercicio e3", "taskflow"]
)
def e3_taskflow():

    @task
    def extraer() -> list[int]:
        return [12, 7, 3, 25, 8, 14]

    @task(multiple_outputs=True)
    # @task
    def estadisticias(numeros: list[int]) -> dict[str, int | float]:
        return {
            "total": sum(numeros),
            "media": sum(numeros) / len(numeros),
            "maximo": max(numeros)
        }

    @task
    # def informe(stats: dict[str, int | float]):
    def informe(total: int | float, media: float, maximo: int | float ):
        print(f"Total={total} | Media={media} | Maximo={maximo}")

    stats = estadisticias(extraer())
    informe(stats['total'], stats['media'], stats['maximo'])


e3_taskflow()
