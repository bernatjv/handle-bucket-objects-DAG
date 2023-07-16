import datetime
import os
from airflow import models
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    "owner": "Composer Example",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": YESTERDAY,
}

with models.DAG(
    "handle_bucket_files",
    catchup=False,
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
    user_defined_macros={
        "BUCKET": YOUR_BUCKET,
        "DAGS_FOLDER": os.environ["DAGS_FOLDER"]
    }
) as dag:
        
    def sort_csv_or_txt_func(**context):
        gcs_objects = context["task_instance"].xcom_pull(task_ids='sense_object_in_bucket')
        csv_files = ["/{}".format(object) for object in gcs_objects if object.split(".")[1] == 'csv']
        txt_files = ["/{}".format(object) for object in gcs_objects if object.split(".")[1] == 'txt']
        txt_files_for_script = " ".join(txt_files)
        context["task_instance"].xcom_push(key="csv_files", value=csv_files)
        context["task_instance"].xcom_push(key="txt_files", value=txt_files)
        context["task_instance"].xcom_push(key="txt_files_for_script", value=txt_files_for_script)
    
    sense_object_in_bucket = GCSObjectsWithPrefixExistenceSensor(
        task_id="sense_object_in_bucket",
        bucket="{{ BUCKET }}",
        prefix=YOUR_PREFIX,
        deferrable=True
    )
    
    sort_csv_or_txt = PythonOperator(
        task_id="sort_csv_or_txt", 
        python_callable=sort_csv_or_txt_func,
    )
    
    copy_txt_to_other_dir = BashOperator(
        task_id="copy_txt_to_other_dir",
        bash_command="bash {{ DAGS_FOLDER }}/scripts/copy_files.sh {{ BUCKET }} {{ task_instance.xcom_pull(task_ids='sort_csv_or_txt', key='txt_files_for_script') }}",
    )
    
    sense_object_in_bucket >> sort_csv_or_txt >> copy_txt_to_other_dir