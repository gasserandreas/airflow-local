import datetime
import pendulum
import os

# from airflow.models.dag import DAG

from airflow.decorators import dag, task

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator

@dag(
    dag_id="process_employees",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessEmployees():
  create_employees_table = SQLExecuteQueryOperator(
    task_id="create_employees_table",
    # postgres_conn_id="postgres_default",
    conn_id="local_postgres",
    sql="""
    CREATE TABLE IF NOT EXISTS employees (
      employee_id SERIAL PRIMARY KEY,
      first_name VARCHAR NOT NULL,
      last_name VARCHAR NOT NULL,
      hire_date DATE NOT NULL,
      termination_date DATE
    );
    """,
  )

  @task
  def echo_something():
    print("something")
  
  create_employees_table >> echo_something()


dag = ProcessEmployees()