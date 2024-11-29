from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default DAG arguments
default_dag = {
    "owner": "Awana",
    "start_date": datetime(2024, 11, 1),  # Static date
    "email": ["test@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,  # Fixed key
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "ETL_toll_data",
    schedule=timedelta(days=1),  # Corrected schedule argument
    default_args=default_dag,
    description="Apache Airflow with Bash",
)

# Task to download data
extract_data = BashOperator(
    task_id="extract_data",
    bash_command="wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz -O /tmp/tolldata.tgz",
    dag=dag,
)


#task to unzip data
unzip_data = BashOperator(
    task_id= "unzip_data",
    bash_command = "tar -xvzf */tolldata.tgz",
    dag = dag
)

# task to extract_data_from_csv
extract_data_from_csv = BashOperator(
    task_id = "extract_data_from_csv",
    bash_command = "cut -d, -f1,2,3,4 */vehicle-data.csv > */csv_data.csv", # -d for delimiter
    dag = dag
)

#task to extract data from tsv
extract_data_from_tsv = BashOperator(
    task_id= "extract_data_from_tsv",
    bash_command = "cut -d$'\t' -f 5,6,7 */tollplaza-data.tsv > */tsv_data.csv", # -d$'\t' for delimiter tab
    dag = dag
)

# Define task pipelines
extract_data >> unzip_data >> extract_data_from_csv >> extract_data_from_tsv