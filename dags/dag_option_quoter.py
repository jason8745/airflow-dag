from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime

default_args = {
    'owner': 'Wen',
    'start_date':datetime(2022, 9, 8, 0, 0),
    }
def option_crawler(**context):
    import bs4
    import pandas as pd
    import requests
    req = requests.post('https://www.taifex.com.tw/cht/3/pcRatioExcel', verify=False)
    req.encoding = 'utf-8'
    soup = bs4.BeautifulSoup(req.text, 'lxml')
    data = soup.select('.table_a')[0]
    df = pd.read_html(str(data),header=0)[0]
    return df

def write_to_influxdb(**context):
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS
    url = Variable.get("influxdb_url")
    token = Variable.get("token")
    org = Variable.get("org")
    bucket = "option"
    # 透過xcom從option_crawler取得df
    df = context['task_instance'].xcom_pull(task_ids='option_crawler')
    with InfluxDBClient(url, token=token, org=org) as client:
        
        write_api = client.write_api(write_options=SYNCHRONOUS)
        data = []
        for  ds in df.values:
            point = Point('option') \
            .tag("type", "sell") \
            .field("volume", float(ds[4])) \
            .time(datetime.strptime(ds[0], "%Y/%m/%d"),WritePrecision.NS)
            data.append(point)
            point = Point('option') \
            .tag("type", "buy") \
            .field("volume", float(ds[5])) \
            .time(datetime.strptime(ds[0], "%Y/%m/%d"),WritePrecision.NS)
            data.append(point)

        write_api.write(bucket, org, data)
            
        
# Dag名稱: option_quoter
with DAG('option_quoter', default_args=default_args,schedule_interval='@daily') as dag:
    option_crawler = PythonOperator(
        task_id='option_crawler',
        python_callable=option_crawler,
        provide_context=True,
    )
    write_task = PythonOperator(
        task_id='write_task',
        python_callable=write_to_influxdb,
        provide_context=True,
    )
		# DAG串聯task
    option_crawler >>write_task