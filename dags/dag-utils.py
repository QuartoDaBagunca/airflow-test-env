from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from datetime import datetime, timedelta
import pendulum
import json

DEFAULT_DATE = pendulum.datetime(2022, 3, 4, tz='America/Toronto')

default_args = {
    'owner': 'user'
    , "depends_on_past": False
    , "start_date": days_ago(2)
    , "email": ["airflow@airflow.com"]
    , "email_on_failure": False
    , "email_on_retry": False
    , 'retries': 2
    , 'retry_delay': timedelta(minutes=2)
}

READ_PRJ_RAW = 'bronze-zone'

READ_PRJ_TRUSTED = 'silver-zone'

WRITE_PRJ_RAW = '?'

WRITE_PRJ_TRUSTED = '?'

FUNCTION_TRIGGER = "fnc-manual-files"

BUCKET_NAME = "gb_data_transfer_files_testes"

DEFAULT_TOLERANCE_DAYS = "30"

URL_FUNCTION = "https://us-central1-?.cloudfunctions.net/"

def sendmessage(taskId, dict_payload):

    new_payload = dict_payload.replace("'", "\"") 

    payload = json.loads(new_payload)

    state = payload['state']
    status = payload['status']
    message = payload['message']

    string_date = (datetime.now() - timedelta(hours=3)).strftime('%d/%m/%Y às %H:%M:%S')

    state = payload['state']
    status = payload['status']
    message = payload['message']

    if state == 'skipped' or state == 'upstream_skipped':
        LoggingMixin().log.info("#### Marks as Skipped")
        raise AirflowSkipException
        
    elif state == 'success':
        payload_slack = {'type': 'mrkdwn','text': message + taskId[12:] + ' finalizada - status ' + status + '\n ' + string_date} 

    if status == '200':
        print(payload_slack)
    else:
        raise AirflowSkipException

with DAG(
    dag_id="dag-utils"
    , default_args=default_args
    , schedule_interval='0 */6 * * *'
    , catchup=False
    , tags=["TESTE", "utilitarios"]
    , description="Teste Operators Pipiline"
) as dag:

    current_date = datetime.now()
    string_date = current_date.strftime("%d-%m-%Y")

    dict_bash = {

      "tab_manual-files_load_minutely": [
        {
            "job_name": "fnc_my_new_func",
            "gcs_origin_folder": ".xlsx",
                "fileserver_path": "",
                "body": {
                }
            }
        ]

    }

    # var_env = json.loads(open("var_env.json", "r").read())
    var_env = json.loads(open("dag_manual-files_load_minutely.json", "r").read())

    READ_PRJ_SENSITIVE_TRUSTED   = var_env["dag_manual-files_load_minutely"]["read_prj_silver"]

    # for param_dict in var_env["tab_manual-files_load_minutely"]:
    for param_dict in dict_bash["tab_manual-files_load_minutely"]:

        # filename_pattern = f"{file_prefix}.csv"

        bucket_name = BUCKET_NAME
        job_name = param_dict['job_name']

        gcs_origin_folder = param_dict['gcs_origin_folder']

        key_folder, filename_pattern = gcs_origin_folder.split('/')
        file_prefix, extension = filename_pattern.split('.')

        file_transfer  = DummyOperator(
            task_id=f"file_transfer_{job_name}", 
            dag=dag
        )

        gcs_sensor = GCSObjectExistenceSensor(
            task_id=f"gcs_sensor_{job_name}",
            bucket=bucket_name,
            object=f"{key_folder}/{file_prefix}.{extension}",
            mode="poke",
            soft_fail=True,
            timeout=5,
            retries=2,
            retry_delay=timedelta(seconds=5),
            dag=dag
        )

        dict_body = json.dumps(param_dict["body"])

        # CALL FUNC MOCK
        bash_command = f"python3 /root/airflow/workspace.py     --request '''{dict_body}'''"

        with TaskGroup(group_id=f'call_function_{file_prefix}') as call_function:

            call_function_request = BashOperator(
                task_id=f"raw_{file_prefix}"
                , bash_command=bash_command
                # , xcon_push=True
            )

            dict_payload =  "{{ ti.xcom_pull(task_ids='call_function_" + f"{file_prefix}.raw_{file_prefix}" + "') }}"

            call_function_response = PythonOperator(
                task_id=f'func_raw_{file_prefix}'
                , python_callable=sendmessage
                , op_kwargs={
                    'taskId': f'func_raw_tb_{file_prefix}'
                    , 'dict_payload': dict_payload
                }
            )

        move_processed  = DummyOperator(
            task_id=f"move_processed_{file_prefix}", 
            dag=dag
        )

        file_transfer >> gcs_sensor >> call_function >> move_processed

if __name__ == "__main__":
    dag.test()