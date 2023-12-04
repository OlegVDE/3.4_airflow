import airflow
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

dag = DAG(
    "hw_3_4_connections",
    default_args={
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    },
    description="import a connection",
    schedule="@once",
    start_date=datetime(2023, 12, 4),
    catchup=False,
    tags=["Data Engineer Course"],
)

t1 = BashOperator(
    task_id="import_a_connection",
    bash_command="airflow connections import /opt/airflow/dags/connections.json",
    dag=dag,
)

t1