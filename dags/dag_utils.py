from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.models import TaskInstance
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from google.auth.transport import requests as auth_requests
from datetime import datetime, timedelta
from google.oauth2 import id_token
import pendulum
import requests
import json

DEFAULT_DATE = pendulum.datetime(2022, 3, 4, tz='America/Toronto')
#Get slack connection information
# slack_webhook_token = BaseHook.get_connection(slack_conn_id).password
# slack_webhook_url = BaseHook.get_connection(slack_conn_id).host
# slack_connection = slack_webhook_url+slack_webhook_token

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

# NOTE: Operator para a separação lógica dos fluxos 

def sendmessage(url, taskId, dict_payload):

    new_payload = dict_payload.replace("'", "\"") 

    payload = json.loads(new_payload)

    state = payload['state']
    status = payload['status']
    message = payload['message']
    erro_message = payload['erro_message']

    # print(">>>>> ENTRANDO <<<<<")

    # print(payload)
    # print(state)

    # print(">>>>> SAINDO <<<<<")

    string_date = (datetime.now() - timedelta(hours=3)).strftime('%d/%m/%Y às %H:%M:%S')
    file_message = "Favor carregar novamente o arquivo"

    state = payload['state']
    status = payload['status']
    message = payload['message']
    erro_message = payload['erro_message']

    if state == 'skipped' or state == 'upstream_skipped':
        LoggingMixin().log.info("#### Marks as Skipped")
        raise AirflowSkipException
        payload_slack = {'type': 'mrkdwn','text': erro_message + taskId[12:] + ' :alert:: - status ' + status + '\n ' + string_date+'. '+file_message} 
        
    elif state == 'success':
        payload_slack = {'type': 'mrkdwn','text': message + taskId[12:] + ' finalizada - status ' + status + '\n ' + string_date} 

    if status == '200':
        print(payload_slack)
    else:
        raise AirflowSkipException

def check_status(**kwargs):

    url = kwargs['slack_connection']
    taskList = kwargs['taskList']
    failCnt = 0

    for dqTask in taskList:
    
        ti = TaskInstance(dqTask, kwargs['execution_date']) 
        state = ti.current_state()
        
        status = "200" if state == "success" else "240"
        payload = {
            "status":f"{status}"
            , "state": f"{state}"
            , "erro_message": ":alert_: Falha na carga da"
            , "message": ":check-icon: Carga da "
        }
        
        if  state == 'failed' or state == 'upstream_failed':
            
            failCnt = failCnt+1            
            sendmessage(url, dqTask.task_id, payload) 
            
        else:
            sendmessage(url, dqTask.task_id, payload) 
            
        
    if failCnt > 0:
        raise ValueError ("Falha Detectada") 
    else:    
        return 'end_monitoring'

with DAG(
    dag_id="dag_utils"
    , default_args=default_args
    , schedule_interval='0 */6 * * *'
    , catchup=False
    , tags=["TESTE", "utilitarios"]
    , description="Teste Operators Pipiline"
) as dag:

    current_date = datetime.now()
    string_date = current_date.strftime("%d-%m-%Y")

    mock_payload = {
        "status":"200"
        , "state": "success"
        , "erro_message": ":alert_: Erro ao carregar aquivo {}. Verifique se o schema do mesmo é compatível com o schema da tabela {} - {} TaskId: ".format('necessidade_abastecimento_eud', 'tb_necessidade_abastecimento_eud', string_date)
        , "message": "Carga da "
    }

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
                    'url': "SLACK_CONNECTION"
                    , 'taskId': f'func_raw_tb_{file_prefix}'
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