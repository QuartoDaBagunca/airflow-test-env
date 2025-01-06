# ====================================================================================================================
# Objeto........: dag_dqi_test_load_daily.py
# Data Criacao..: 21/10/2024
# Descricao.....: DAG de teste para execução de procedure modelo do DQ Interno
# Autor.........: Diego Ramos
# =====================================================================================================================

import os
import json
import airflow
import argparse
import requests
# from libs.airflow import log
from textwrap import dedent 
from datetime import timedelta,datetime
from airflow.utils.dates import days_ago
from airflow.models import TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
# from libs.provider.grupoboticario.utils.variable import Variable
from airflow.providers.google.cloud.operators.bigquery  import BigQueryExecuteQueryOperator
# from libs.provider.grupoboticario.utils.identificador_plataforma import IdentificadorPlataforma
from slack_sdk.errors import SlackApiError
from airflow.utils.task_group import TaskGroup
from airflow.providers.slack.operators.slack_webhook import \
    SlackWebhookOperator

# env_var = Variable.get("dag_dqi_test_load_daily", deserialize_json=True)
env_var = json.loads(open("/root/airflow/dags/aux/dag_dqi_test_load_daily.json", "r").read())["dag_dqi_test_load_daily"]
slack_conn_id               = env_var["slack_connection"]
slack_token                 = os.environ['AIRFLOW_CONN_SLACK_F_MANUAL_NOTIFY']
SCHEDULE_INTERVAL 			= env_var["schedule_interval"]
DAG_DESC 					= env_var["description"]
ERROR_NOTIFICATION 			= env_var["error_notification"]
SUCCE_NOTIFICATION 			= env_var["success_notification"]
ERROR_DQI_NOTIFICATION 		= env_var["error_dqi_notification"]

WRITE_PRJ_RAW               = env_var["write_prj_raw"]
WRITE_PRJ_RAW_CUSTOM        = env_var["write_prj_raw_custom"]
WRITE_PRJ_TRUSTED 			= env_var["write_prj_trusted"]
WRITE_PRJ_REFINED 			= env_var["write_prj_refined"]

READ_PRJ_RAW                = env_var["read_prj_raw"]
READ_PRJ_RAW_CUSTOM         = env_var["read_prj_raw_custom"]
READ_PRJ_TRUSTED 			= env_var["read_prj_trusted"]
READ_PRJ_REFINED 			= env_var["read_prj_refined"]

def run_task(dag, task_id):
    task = dag.get_task(task_id)
    task.run(start_date=datetime.now(), end_date=datetime.now())

def failure_alert_slack(context):

    message = ""

    task_id = context['task_instance'].task_id
    if 'end_ci_process_fail' in task_id:
        message = f"""
            {fail_message}\n\n{(datetime.now() - timedelta(hours=3)).strftime('%d/%m/%Y às %H:%M:%S')}
        """
    else:
        message = str(context.get("exception"))

    payload = {"text": message}

    try:
        response = requests.post(slack_token, json=payload)
    except SlackApiError as e:
        print(f"""status:{response.raise_for_status()}""")
        assert e.response["error"]

# def check_status(**kwargs):
def custom_error_msg(context):

    state = context['task_instance'].current_state
    erro_msg = context.get('exception')
    task_id = context['task_instance'].task_id
        
    if 'failed' in str(state) and 'DQ-INTERNO' in str(erro_msg):
        raise ValueError (ERROR_DQI_NOTIFICATION.format(str(erro_msg), task_id))
    else:    
        return 'end_monitoring'
    
default_args = {

    "start_date": days_ago(2)
    , 'owner': 'user'
    , "depends_on_past": False
    # identificador_plataforma.get_owner()
    # , "on_failure_callback": slack_notification.send_fail_alert
    # , "on_success_callback": log
    # , "on_retry_callback":   log
    # , "sla_miss_callback":   log
    , "identificador_plataforma_labels": ["Carga Critica"]# identificador_plataforma.get_labels()
    , "retries": env_var["retries"]
    , 'retry_delay': timedelta(minutes=2)
}

with airflow.DAG(

    "dag_dqi_test_load_daily"
	, description=DAG_DESC
    , default_args=default_args
    #, tags=identificador_plataforma.get_tags()
    , dagrun_timeout=timedelta(minutes=240)
    , schedule_interval=SCHEDULE_INTERVAL
    , catchup=False
    , max_active_runs = 1
) as dag:
    
    prc_load_tb_phase_in_out_so_cp = BigQueryExecuteQueryOperator(
        task_id='prc_load_tb_phase_in_out_so_cp',
        sql="CALL `{}.sp.prc_load_tb_phase_in_out_so_cp`('{}', '{}', '{}', '{}', '{}')".format(
            # project_trusted, project_raw, project_raw_custom, project_trusted, project_refined, project_sensitive_trusted),
            WRITE_PRJ_TRUSTED, WRITE_PRJ_RAW, WRITE_PRJ_RAW_CUSTOM, WRITE_PRJ_TRUSTED, WRITE_PRJ_REFINED, WRITE_PRJ_REFINED),
        use_legacy_sql=False,
        priority="BATCH",
        on_failure_callback=custom_error_msg,
        dag=dag,
        depends_on_past=False
    )

    # query = f"""

    #     DECLARE VAR_PRJ_RAW STRING DEFAULT NULL;
    #     DECLARE VAR_PRJ_RAW_CUSTOM STRING DEFAULT NULL;
    #     DECLARE VAR_PRJ_TRUSTED STRING DEFAULT NULL;
    #     DECLARE VAR_PRJ_REFINED STRING DEFAULT NULL;
    #     CALL `{WRITE_PRJ_TRUSTED}.sp.prc_load_teste_dq_interno`(VAR_PRJ_RAW, VAR_PRJ_RAW_CUSTOM, VAR_PRJ_TRUSTED, VAR_PRJ_REFINED);
    # """

    dag.doc_md = dedent(DAG_DESC)

    start_process = DummyOperator(task_id="start_process")
    
    # task = BigQueryExecuteQueryOperator(
    #     task_id='test_task',
    #     sql=query,
    #     use_legacy_sql=False,
    #     priority="BATCH",
    #     on_failure_callback=custom_error_msg,
    #     dag=dag,
    #     depends_on_past=False
    # )

    vds_tb_meta_sugestao = DummyOperator(task_id="vds_tb_meta_sugestao")
    # vds_tb_meta_sugestao = ValidadorDadosStartExecutionOperator(
    #     task_id="vds_refined_abastecimento_tb_meta_sugestao"
    #     , dataset_name=f"{WRITE_PRJ_REFINED}.abastecimento.tb_meta_sugestao"
    #     , dag=dag
    #     , timeout=timedelta(minutes=10)
    #     , timeout_behavior='mark_task_as_failed'
    # )

    vdc_tb_meta_sugestao = DummyOperator(task_id="vdc_tb_meta_sugestao")
    # vdc_tb_meta_sugestao = ValidadorDadosCheckExecutionSensor(
    #     task_id="vdc_refined_abastecimento_tb_meta_sugestao"
    #     , dataset_name=f"{WRITE_PRJ_REFINED}.abastecimento.tb_meta_sugestao"
    #     , dag=dag
    #     , timeout=timedelta(minutes=10)
    #     , timeout_behavior='mark_task_as_failed'
    # )

    # ===========================================================================================
    #region SLACK
    with TaskGroup("SLACK_NOTIFICATION") as tg_slack:

        message = f":rotating_light: *refined-zone.abastecimento.tb_meta_sugestao*: {ERROR_NOTIFICATION}."
        fail_message = f":alert: *refined-zone.abastecimento.tb_meta_sugestao*: {SUCCE_NOTIFICATION}"

        end_ci_process_message_fail = SlackWebhookOperator(
            task_id=f"end_ci_process_fail"
            , slack_webhook_conn_id=slack_conn_id
            , trigger_rule=TriggerRule.ONE_FAILED
            , message = f"""
                {fail_message}\n\n{(datetime.now() - timedelta(hours=3)).strftime('%d/%m/%Y às %H:%M:%S')}
            """
            , on_failure_callback=failure_alert_slack
            , dag=dag
        )

        end_ci_process_message_sucssess = SlackWebhookOperator(
            task_id=f"end_ci_process_sucssess"
            , slack_webhook_conn_id=slack_conn_id
            , trigger_rule=TriggerRule.ALL_SUCCESS
            , message = f"""
                {message}\n\n{(datetime.now() - timedelta(hours=3)).strftime('%d/%m/%Y às %H:%M:%S')}
            """
            , dag=dag
        )
        
    #endregion
    # ===========================================================================================

    start_process >> prc_load_tb_phase_in_out_so_cp >> vds_tb_meta_sugestao >> vdc_tb_meta_sugestao# >> tg_slack

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Execute a specific task in the DAG.')
    parser.add_argument('--task_id', type=str, help='The task_id of the task to execute')
    args = parser.parse_args()

    if args.task_id:
        run_task(dag, args.task_id)
    else:
        dag.test()