
# [START download_stock_price]
# [START import_module]
from datetime import timedelta
from textwrap import dedent
import os
import yfinance as yf
import mysql.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable


# [END import_module]

# [START default_args]
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
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

# 下载股票信息并保存到文件
def download_price(**context):
    stock_list = context["dag_run"].conf.get("stocks")

    for ticker in stock_list:
        msft = yf.Ticker(ticker)
        hist = msft.history(period="1mo")
        # print(type(hist))
        # print(hist.shape)
        # print(hist)

        print(os.getcwd())
        with open(get_file_path(ticker), 'w') as writer:
            hist.to_csv(writer, index=True)
        print(f"Downloaded {ticker}")

# download_price()


def get_file_path(ticker):
    return f'/home/ec2-user/app/py-project/data/{ticker}.csv'

    
def load_price_data(ticker):
    with open(get_file_path(ticker), 'r') as reader:
        lines = reader.readlines()
        return [[ticker]+line.split(',')[:5] for line in lines if line[:4] != 'Date']

# 读取文件写入数据库中
def save_to_mysql_stage(*args, **context):
    stock_list = context["dag_run"].conf.get("stocks")

    # 连接mysql
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Reyun.123",
        database="demodb",
        port=3306
    )

    mycursor = mydb.cursor()
    for ticker in stock_list:
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
    'download_stock_price_v4',
    default_args=default_args,
    description='Download stock price and save to local csv files',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
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

    # 依赖关系
    download_task >> save_to_mysql_task




# [END download_stock_price]