from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
with DAG(
        dag_id='infra_heartbeat',
        start_date=datetime(2026, 3, 23),
        schedule_interval='@once',
        catchup=False
) as dag:
    test_task = BashOperator(
        task_id='echo_test',
        bash_command='echo "Airflow is connected to the project folder!"'
    )