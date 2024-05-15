from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.models import Variable
from kubernetes.client import models as k8s
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}
with DAG(
   'my-second-dag',
   default_args=default_args,
   description='simple dag',
   schedule_interval=timedelta(days=1),
   start_date=datetime(2024, 3, 12),
   catchup=False,
   tags=['example11']
) as dag:
   start = EmptyOperator(task_id="start")
   t1 = SparkKubernetesOperator(
       task_id='n-spark-pi',
       trigger_rule="all_success",
       depends_on_past=False,
       retries=3,
       application_file="sparkjob.yaml",
       namespace="spark-jobs",
       kubernetes_conn_id="myk8s",
       do_xcom_push=True,
       dag=dag
   )
   start >> t1
    
