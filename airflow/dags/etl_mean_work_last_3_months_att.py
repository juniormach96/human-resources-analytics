import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from minio import Minio

DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 1, 13),
}

dag = DAG('etl_mean_work_last_3_months_att',
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
    df_working_hours = pd.DataFrame(
        data=None, columns=["emp_id", "data", "hora"])

    # Search objects on the data lake
    objects = client.list_objects('landing', prefix='working-hours',
                                  recursive=True)

    # Download each file and concatenate with the empty dataframe or the last one
    for obj in objects:

        print("Downloading file...")
        print(obj.bucket_name, obj.object_name.encode('utf-8'))

        obj = client.get_object(
            obj.bucket_name,
            obj.object_name.encode('utf-8'),
        )
        data = obj.read()
        df_ = pd.read_excel(data)
        df_working_hours = pd.concat([df_working_hours, df_])

    df_working_hours.to_csv("/tmp/mean_work_last_3_months.csv", index=False
                            )


def transform():

    df_ = pd.read_csv("/tmp/mean_work_last_3_months.csv")

    # Converts data to numeric and datetime formats
    df_["hora"] = pd.to_numeric(df_["hora"])
    df_["data"] = pd.to_datetime(df_["data"])

    # Filters only records from the last 3 months
    three_months_ago = df_['data'].max() - pd.DateOffset(months=3)
    df_last_3_month = df_[(df_['data'] > three_months_ago)]

    # Calculates the average working hours in the last months
    mean_work_last_3_months = df_last_3_month.groupby("emp_id")[
        "hora"].agg("sum")/3

    # Create the dataframe with the transformed data
    mean_work_last_3_months = pd.DataFrame(data=mean_work_last_3_months)
    mean_work_last_3_months.rename(
        columns={"hora": "mean_work_last_3_months"}, inplace=True)

    mean_work_last_3_months.to_csv(
        "/tmp/mean_work_last_3_months.csv", index=False
    )


def load():

    # Load data
    df_ = pd.read_csv("/tmp/mean_work_last_3_months.csv")

    # Convert to parquet
    df_.to_parquet(
        "/tmp/mean_work_last_3_months.parquet", index=False
    )

    # Export to Data Lake
    client.fput_object(
        "processing",
        "mean_work_last_3_months.parquet",
        "/tmp/mean_work_last_3_months.parquet"
    )


extract_task = PythonOperator(
    task_id='extract_file_from_data_lake',
    provide_context=True,
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    provide_context=True,
    python_callable=transform,
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

extract_task >> transform_task >> load_task >> clean_task
