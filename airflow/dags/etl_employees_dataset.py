from datetime import datetime, date, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from minio import Minio


DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 13),
}

dag = DAG('etl_employees_dataset',
          default_args=DEFAULT_ARGS,
          schedule_interval="@once"
          )

data_lake_server = Variable.get("data_lake_server")
data_lake_login = Variable.get("data_lake_login")
data_lake_password = Variable.get("data_lake_password")

client = Minio(
    data_lake_server,
    access_key=data_lake_login,
    secret_key=data_lake_password,
    secure=False
)


def extract():
    # Creates an empty df
    df = pd.DataFrame(data=None)

    # Search objects on the data lake
    objects = client.list_objects('processing', recursive=True)

    # Download each file and concatenate with the empty dataframe or the last one
    for obj in objects:
        print("Downloading file...")
        print(obj.bucket_name, obj.object_name.encode('utf-8'))

        # Download data of an object to file
        client.fget_object(
            obj.bucket_name,
            obj.object_name.encode('utf-8'),
            "/tmp/temp_.parquet",
        )
        df_temp = pd.read_parquet("/tmp/temp_.parquet")
        # Concat the df with the last one
        # We can use concat method because all the dataframes are ordered by emp_id column
        # Other option would be persisting emp_id attribute on all dataframes and merging the files
        df = pd.concat([df, df_temp], axis=1)

    df.to_csv("/tmp/employees_dataset.csv", index=False
              )


def load():

    # Load data
    df_ = pd.read_csv("/tmp/employees_dataset.csv")

    # Convert to parquet
    df_.to_parquet(
        "/tmp/employees_dataset.parquet", index=False
    )

    # Export to Data Lake
    client.fput_object(
        "processing",
        "employees_dataset.parquet",
        "/tmp/employees_dataset.parquet"
    )


extract_task = PythonOperator(
    task_id='extract_data_from_datalake',
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

# Remove temporary files
clean_task = BashOperator(
    task_id="clean_files_on_staging",
    bash_command="rm -f /tmp/*.csv;rm -f /tmp/*.json;rm -f /tmp/*.parquet;",
    dag=dag
)

extract_task >> load_task >> clean_task
