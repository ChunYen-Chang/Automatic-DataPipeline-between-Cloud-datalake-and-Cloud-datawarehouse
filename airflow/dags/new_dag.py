import airflowlib.emr_lib as emrlib
#import os
import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'ChunYen-Chang',
    'depends_on_past': False,
    'start_date': datetime(2019,8,30),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}

dag = DAG('I94Immigration', concurrency=1, schedule_interval=None, default_args=default_args)

region_name = 'us-west-2'
keyid = 'AKIAIJS2VFNEIVPJAOPQ'
skey = 'zWLTf6l4oK5E011wf0fjmSiYGDqHtxHpvCXetX19'


emrlib.client()


def create_emr(**kwargs):
    cluster_id = emrlib.create_cluster(region_name='us-west-2', cluster_name='cluster', num_core_nodes=2)
    return cluster_id


def wait_for_completion(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emrlib.wait_for_cluster_creation(cluster_id)
    
# Terminates the EMR cluster
def terminate_emr(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emrlib.terminate_cluster(cluster_id)


create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_emr,
    dag=dag)

wait_for_cluster_completion = PythonOperator(
    task_id='wait_for_cluster_completion',
    python_callable=wait_for_completion,
    dag=dag)

terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_done',
    dag=dag)

create_cluster >> wait_for_cluster_completion
wait_for_cluster_completion >> terminate_cluster
