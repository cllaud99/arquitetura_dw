from datetime import datetime, timezone
import pytz

# Airflow
from airflow.sdk import dag, task
from airflow.operators.empty import EmptyOperator  # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    dag_id="test-con",
    description="Migração direta de contratos bronze para a silver usando SQL via PostgresHook",
    schedule=None,
    start_date=datetime(2025, 6, 4),
    catchup=False,
    tags=["mk", "postgres", "teste"],
)
def main():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def run_test():
        pg_hook = PostgresHook(postgres_conn_id="dev-postgres")

        sql_query = """
        SELECT 1;
        """

        pg_hook.run(sql_query)
        print("✅ Inserção executada com sucesso.")

    executar = run_test()
    start >> executar >> end

dag = main()
