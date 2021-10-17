
# [START download_stock_price]
# [START import_module]
from datetime import timedelta
from textwrap import dedent
import os
import yfinance as yf
import mysql.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable


# [END import_module]

# [START default_args]
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email': ['jizhe0910@163.com'], # 配置通知邮件
    'email_on_failure': True, # 失败通知
    'email_on_retry': True, # 重试通知
    'retries': 1, # 重试次数
    'retry_delay': timedelta(seconds=30), # 重试间隔时间
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
# [END default_args]



# [START instantiate_dag]
with DAG(
    'download_stock_list_trigger_next_dag',
    default_args=default_args,
    description='Download stock price and save to local csv files',
    schedule_interval="5 5 * * *',
    start_date=days_ago(2),
    tags=['example'],
    catchup=False,
    max_active_runs=1, # 同时只允许一个DAGRun在运行
) as dag:
    # [END instantiate_dag]


    dag.doc_md = """
    This DAG download stock price
    """

    # task定义  Sensor传感器使用 监测s3某个文件夹下的文件是否存在
    s3_sensor = S3KeySensor(
        task_id = "new_s3_file",
        bucket_key = 'airflow/stockprices/{{ds_nodash}}/*.csv',
        wildcard_match = True # 通配符匹配
        bucket_name = 'test', # S3 桶名
        aws_conn_id = 'aws_s3', # conn 连接信息
        timeout = 18 * 60 * 60, # 超时
        poke_interval = 30, # 传感器监测时间间隔
        dag = dag, 
    )

    # task定义 列出s3某个前缀下的文件
    list_s3_file = S3ListOperator(
        task_id = "list_s3_files",
        bucket_name = 'test', # S3 桶名
        prefix = 'airflow/stockprices/20210910',
        delimiter = '/',
        aws_conn_id = 'aws_s3', # conn 连接信息
    )

    # task 定义 触发其他Dag
    trigger_next_dag = TriggerDagRunOperator(
        trigger_dag_id = "download_stock_price_v1", # 触发Dag的ID
        task_id = "download_price", # 触发Task ID
        execution_date = "{{ds}}", # 执行日期
        wait_for_completion = Fasle
    )

    # 依赖关系
    s3_sensor >> list_s3_file >> trigger_next_dag 


# [END download_stock_price]