from datetime import datetime, timedelta
import argparse
import json
import os
import os
import subprocess

help_unid = " Definindo qual unidade de negocio sera passado para o script Ex: --unid bot"
help_job = " Definindo qual o script que sera executado (FP ou CI) Ex:  --job ci"
parser = argparse.ArgumentParser()
parser.add_argument("--unid", help=help_unid)
parser.add_argument("--job", help=help_job)
args = parser.parse_args()
if args.unid or args.job:
    unid = args.unid
    job = args.job

var_env = json.loads(open("/root/airflow/dags/dag_cc_teste.json", "r").read())

param_var_dag_dict = var_env["var_dag_teste"]


list_unid = var_env["var_list_unid"]

var_env = str(param_var_dag_dict).replace("'", "\"")

bucket_folder = f"{param_var_dag_dict['spark_bucket']}/{param_var_dag_dict['spark_folder']}"
                                                                                                                                          
cmd_ci = f""" 
gcloud beta dataproc jobs submit pyspark \
    --cluster {param_var_dag_dict['cluster_name']} \
    --region us-east1 \
    --labels "job=cc_{unid}" \
    --project {param_var_dag_dict['spark_project_id']} \
    "gs://{bucket_folder}/compra_inteligente.py" \
    --jars "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar" \
    --py-files "gs://{bucket_folder}/compra_inteligente_{list_unid[0]}.py"\
,"gs://{bucket_folder}/helper_pyspark.py"\
,"gs://{bucket_folder}/compra_inteligente_{list_unid[1]}.py"\
,"gs://{bucket_folder}/compra_inteligente_{list_unid[2]}.py"\
,"gs://{bucket_folder}/compra_inteligente_{list_unid[3]}.py" -- '{unid.upper()}' \
'{var_env.replace("None", "null")}'
"""

cmd_fp = f"""
gcloud beta dataproc jobs submit pyspark \
    --cluster {param_var_dag_dict['cluster_name']} \
    --region us-east1 \
    --labels "job=cc_{unid}" \
    --project {param_var_dag_dict['spark_project_id']} \
    "gs://{bucket_folder}/fator_proporcional.py" \
    --jars "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar" \
    --py-files "gs://{bucket_folder}/fator_proporcional_{list_unid[0]}.py"\
,"gs://{bucket_folder}/helper_pyspark.py"\
,"gs://{bucket_folder}/fator_proporcional_{list_unid[1]}.py"\
,"gs://{bucket_folder}/fator_proporcional_{list_unid[2]}.py"\
,"gs://{bucket_folder}/fator_proporcional_{list_unid[3]}.py" -- '{unid.upper()}' \
'{var_env.replace("None", "null")}'
"""

def sample_submit_job():

    cmd = cmd_ci if job == "ci" else cmd_fp
    try:

        shellcmd = os.popen(cmd)
        print(shellcmd.read())
    except Exception as Err:

        print(Err)

def get_gcloud_project():
    try:
        project = subprocess.check_output(['gcloud', 'config', 'get-value', 'project'], universal_newlines=True).strip()
        return project
    except subprocess.CalledProcessError as e:
        print(f"Error: {e.output}")
        return None

if __name__ == "__main__":
    sample_submit_job()