from datetime import datetime
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, chain

import random


@dag(
    schedule=None,
    start_date=datetime(2026, 10, 23),
    catchup=False,
    tags=["ejercicio e3", "paralelo"]
)
def e3_paralelo():
    inicio = EmptyOperator(task_id="inicio")

    extraer_crm = BashOperator(
        task_id="extraer_crm",
        bash_command=f"sleep {random.randint(5, 50)} && echo 'extrayendo datos de crm'"
    )
    extraer_erp = BashOperator(
        task_id="extraer_erp",
        bash_command=f"sleep {random.randint(5, 50)} && echo 'extrayendo datos de erp'"
    )
    extraer_web = BashOperator(
        task_id="extraer_web",
        bash_command=f"sleep {random.randint(5, 50)} && echo 'extrayendo datos de web'"
    )

    unir = BashOperator(
        task_id="unir",
        bash_command="echo 'uniendo 3 fuentes'"
    )

    fin = EmptyOperator(task_id="fin")

    # forma legacy / antigua
    inicio >> [extraer_crm, extraer_erp, extraer_web] >> unir >> fin

    # forma moderna
    # chain(inicio, [extraer_crm, extraer_erp, extraer_web], unir, fin)


e3_paralelo()
