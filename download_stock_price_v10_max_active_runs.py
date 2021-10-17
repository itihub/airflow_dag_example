
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

# 下载股票信息并保存到文件,返回有效的股票信息 PythonOperator返回直接存储在xcom中
def download_price(**context):
    stock_list = context["dag_run"].conf.get("stocks")
    valid_tickers = []

    for ticker in stock_list:
        msft = yf.Ticker(ticker)
        hist = msft.history(period="1mo")
        if hist.shape[0]>0:
            valid_tickers.append(ticker)
        else:
            continue
        # print(type(hist))
        # print(hist.shape)
        # print(hist)

        print(os.getcwd())
        with open(get_file_path(ticker), 'w') as writer:
            hist.to_csv(writer, index=True)
        print(f"Downloaded {ticker}")
    
    print(f"returned tickers: {valid_tickers}")
    return valid_tickers

# download_price()


def get_file_path(ticker):
    # 设置不存在的路径 让任务失败
    return f'/home/ec2-user/app/py-project/aadata/{ticker}.csv'

    
def load_price_data(ticker):
    with open(get_file_path(ticker), 'r') as reader:
        lines = reader.readlines()
        return [[ticker]+line.split(',')[:5] for line in lines if line[:4] != 'Date']

# 读取文件写入数据库中
def save_to_mysql_stage(*args, **context):
    # tickers = get_tickers(context)
    tickers = context['ti'].xcom_pull(task_ids='download_price') # 从xcom中获取股票数据
    print(f"received tickers: {tickers}")

    # 从connection获取数据库配置 需要去Admin->Connections里面配置
    from airflow.hooks.base import BaseHook
    conn = BaseHook.get_connection('demodb')
    mydb = mysql.connector.connect(
        host=conn.host,
        user=conn.login,
        password=conn.password,
        database=conn.schema,
        port=conn.port
    )

    mycursor = mydb.cursor()
    for ticker in tickers:
        val = load_price_data(ticker)
        print(f"{ticker} length={len(val)}  {val[1]}")

        sql = """
        INSERT INTO stock_prices_stage(ticker, as_of_date, open_price, high_price, low_price, close_price)
        VALUES(%s, %s, %s, %s, %s, %s)
        """
        mycursor.executemany(sql, val)

        mydb.commit()

        print(mycursor.rowcount, "record inserted.")


# [START instantiate_dag]
with DAG(
    'download_stock_price_v10_max_active_runs',
    default_args=default_args,
    description='Download stock price and save to local csv files',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
    catchup=False,
    max_active_runs=1, # 同时只允许一个DAGRun在运行
) as dag:
    # [END instantiate_dag]


    dag.doc_md = """
    This DAG download stock price
    """

    # task定义
    download_task = PythonOperator(
        task_id = "download_price",
        python_callable = download_price,
        provide_context = True
    )

    # task定义
    save_to_mysql_task = PythonOperator(
        task_id = "save_to_mysql_stage",
        python_callable = save_to_mysql_stage,
        provide_context = True
    )

    # task定义
    mysql_task = MySqlOperator(
        task_id = "merge_stock_price",
        mysql_conn_id = 'demodb',
        sql = 'merge_stock_price.sql',
        dag = dag,
    )

    email_task = EmailOperator(
        task_id = 'send_email',
        to = 'jize0910@163.com',
        subject = 'Stock Price is downloaded',
        html_content = """
        <h3>Email Test</h3> 
        {{ ds_nodash }} <br/> 
        {{ dag }} <br/> 
        {{ conf }} <br/> 
        {{ next_ds }} <br/>
        {{ yesterday_ds }} <br/>
        {{ tomorrow_ds }} <br/>
        {{ execution_date }}  <br/>
        """,
        dag = dag
    )

    # 依赖关系
    download_task >> save_to_mysql_task >> mysql_task >> email_task


# [END download_stock_price]