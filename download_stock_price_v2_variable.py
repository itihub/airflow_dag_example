
# [START download_stock_price]
# [START import_module]
from datetime import timedelta
from textwrap import dedent
import os
import yfinance as yf
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
def download_price():
    stock_list_str = Variable.get("stock_list") # 获取Variable变量
    stock_list_json = Variable.get("stock_list_json", deserialize_json=True) # 获取Variable变量 序列化成JSON
    print(stock_list_str)
    print(stock_list_json)

    for ticker in stock_list_json:
        msft = yf.Ticker(ticker)
        hist = msft.history(period="max")
        # print(type(hist))
        # print(hist.shape)
        # print(hist)

        print(os.getcwd())
        with open(f'/home/ec2-user/app/py-project/data/{ticker}.csv', 'w') as writer:
            hist.to_csv(writer, index=True)
        print("Finished downloading price data for " + ticker)

# download_price()

# [START instantiate_dag]
with DAG(
    'download_stock_price_v2_variable',
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

    # task 定义
    download_task = PythonOperator(
        task_id = "download_price",
        python_callable = download_price
    )





# [END download_stock_price]