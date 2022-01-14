from datetime import datetime, date, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from minio import Minio
from sqlalchemy.engine import create_engine


DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 13),
}

dag = DAG('etl_number_projects_att',
          default_args=DEFAULT_ARGS,
          schedule_interval="@once"
          )

data_lake_server = Variable.get("data_lake_server")
data_lake_login = Variable.get("data_lake_login")
data_lake_password = Variable.get("data_lake_password")

database_server = Variable.get("database_server")
database_login = Variable.get("database_login")
database_password = Variable.get("database_password")
database_name = Variable.get("database_name")


url_connection = f"mysql+pymysql://{database_login}:{database_password}@{database_server}/{database_name}"
engine = create_engine(url_connection)

client = Minio(
    data_lake_server,
    access_key=data_lake_login,
    secret_key=data_lake_password,
    secure=False
)


def extract():

    query = """SELECT Count(PROJECT_ID) as number_projects
            FROM projects_emp
            GROUP BY (emp_id);"""

    df_ = pd.read_sql_query(query, engine)

    df_.to_csv("/tmp/number_projects.csv", index=False
               )


def load():

    # Load data
    df_ = pd.read_csv("/tmp/number_projects.csv")

    # Convert to parquet
    df_.to_parquet(
        "/tmp/number_projects.parquet", index=False
    )

    # Export to Data Lake
    client.fput_object(
        "processing",
        "number_projects.parquet",
        "/tmp/number_projects.parquet"
    )


extract_task = PythonOperator(
    task_id='extract_data_from_database',
    provide_context=True,
    python_callable=extract,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_file_to_data_lake',
    provide_context=True,
    python_callable=load,
    dag=dag
)

clean_task = BashOperator(
    task_id="clean_files_on_staging",
    bash_command="rm -f /tmp/*.csv;rm -f /tmp/*.json;rm -f /tmp/*.parquet;",
    dag=dag
)

extract_task >> load_task >> clean_task
