from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'a_0_connection_test',
    start_date=datetime(2026, 3, 8),
    schedule=None,
    catchup=False
) as dag:

    # This just prints "Hello" and the current date to the Airflow logs
    test_task = BashOperator(
        task_id='verify_stadium_links',
        bash_command='echo "Stadium is Open. Links are Active. Date: $(date)"'
    )

