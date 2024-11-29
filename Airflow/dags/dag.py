from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
with DAG(
     dag_id="my_dag_name",
     start_date=datetime(2021, 1, 1),
     schedule="@once",
 ):
     EmptyOperator(task_id="task")