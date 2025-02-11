"""
### Create Demo Table
Documentation that goes along with the Airflow tutorial located
"""

import os
from datetime import timedelta
from airflow.operators.sql import SQLExecuteQueryOperator
from airflow import DAG

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "postgres_operator_dag"

# [START instantiate_dag]
with DAG(
    'create-demo-table',
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
) as dag:
    # create table
    create_table = SQLExecuteQueryOperator(
      task_id="create_demo_table",
      conn_id="postgres_default",
      sql="sql/create_demo_table.sql",
    )


