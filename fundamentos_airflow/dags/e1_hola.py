from datetime import datetime

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator


@dag(
    schedule="@daily",
    start_date=datetime(2026, 10, 23),
    catchup=False,
    tags=["ejercicio e1"]
)
def e1_hola():
    pass

e1_hola()
