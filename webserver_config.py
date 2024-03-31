import argparse
import json
import os

help = " Definindo qual unidade de negocio sera passado para o script "
parser = argparse.ArgumentParser()
parser.add_argument("--unid", help=help)
args = parser.parse_args()
if args.unid:
    unid = args.unid

var_env = json.loads(open("dags/dag_cc_teste.json", "r").read())

param_var_dag_dict = var_env["var_dag_teste"]

list_unid = var_env["var_list_unid"]

var_env = str(param_var_dag_dict).replace("'", "\"")

bucket_folder = f"{param_var_dag_dict['spark_bucket']}/{param_var_dag_dict['spark_folder']}"

cmd = f"""
gcloud beta dataproc jobs submit pyspark \
    --cluster {param_var_dag_dict['cluster_name']} \
    --region us-east1 \
    --labels "job=cc_{unid}" \
    --project {param_var_dag_dict['spark_project_id']} \
    "gs://{bucket_folder}/calculo_compra.py" \
    --jars "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.30.0.jar" \
    --py-files "gs://{bucket_folder}/calculo_compra_{list_unid[0]}.py"\
,"gs://{bucket_folder}/helper_pyspark.py"\
,"gs://{bucket_folder}/calculo_compra_{list_unid[1]}.py"\
,"gs://{bucket_folder}/calculo_compra_{list_unid[2]}.py"\
,"gs://{bucket_folder}/calculo_compra_{list_unid[3]}.py" -- '{unid.upper()}' \
'{var_env.replace("None", "null")}'
"""

def sample_submit_job():

    try:

        shellcmd = os.popen(cmd)
        print(shellcmd.read())
    except Exception as Err:

        print(Err)

if __name__ == "__main__":
    sample_submit_job()
    # print(bucket_folder)