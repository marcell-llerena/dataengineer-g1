from datetime import datetime
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, chain

import random


@dag(
    schedule=None,
    start_date=datetime(2026, 10, 23),
    catchup=False,
    tags=["ejercicio e2", "paralelo"]
)
def e2_paralelo():
    inicio = EmptyOperator(task_id="inicio")

    # extraer_crm = BashOperator()
    # extraer_erp = BashOperator()

    extracciones = [
        BashOperator(
            task_id=f"extraer_{fuente}",
            bash_command=f"sleep {random.randint(5, 50)} && echo 'extrayendo datos de {fuente}'"
        )
        for fuente in ["crm", "erp", "web"]
    ]

    unir = BashOperator(
        task_id="unir",
        bash_command="echo 'uniendo 3 fuentes'"
    )

    fin = EmptyOperator(task_id="fin")

    # forma legacy / antigua
    # inicio >> extracciones >> unir >> fin

    chain(inicio, extracciones, unir, fin)


e2_paralelo()
