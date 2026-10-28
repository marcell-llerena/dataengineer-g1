from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.sdk import dag, task


@dag(
    schedule=None,
    start_date=datetime(2026, 10, 27),
    catchup=False,
    tags=["ejercicio e5", "sql"]
)
def e5_sql():

    crear = SQLExecuteQueryOperator(
        task_id="crear_tabla",
        conn_id="postgres_dwh",
        sql="CREATE TABLE IF NOT EXISTS clientes(id INT PRIMARY KEY, nombre TEXT, ciudad TEXT)"
    )

    insertar = SQLExecuteQueryOperator(
        task_id="insertar",
        conn_id="postgres_dwh",
        sql="""INSERT INTO clientes (id, nombre, ciudad) VALUES
               (1, 'Ana', 'Arequipa'),
               (2, 'Luis', 'Lima'),
               (3, 'Marta', 'Tacna')
               ON CONFLICT (id) DO NOTHING;
            """
    )

    @task
    def contar():
        hook = PostgresHook(postgres_conn_id="postgres_dwh")
        total = hook.get_first("SELECT COUNT(1) FROM clientes")[0]
        # clientes = hook.get_first("SELECT nombre FROM clientes WHERE ciudad = 'Lima'")[0]
        # print(f"Clientes: {clientes}")
        print(f"Hay {total} de clientes")
        return total


    crear >> insertar >> contar()


e5_sql()
