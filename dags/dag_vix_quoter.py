from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime

default_args = {
    'owner': 'Wen',
    'start_date':datetime(2022, 8, 8, 0, 0),
    }
def vix_crawler(**context):
    import requests
    import pandas as pd
    import io
    
    csv_url = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
    try:
        content = requests.get(csv_url)
        df = pd.read_csv(io.StringIO(content.text), sep=",")
    except Exception as e :
        print(e)
    return df

def write_to_influxdb(**context):
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS
    url = Variable.get("influxdb_url")
    token = Variable.get("token")
    org = Variable.get("org")
    bucket = "index"
		# 透過xcom從vix_crawler取得df
    df = context['task_instance'].xcom_pull(task_ids='vix_crawler')
    with InfluxDBClient(url, token=token, org=org) as client:
        try:
            write_api = client.write_api(write_options=SYNCHRONOUS)
            data = []              
            for ind , ds in enumerate(df.values):
                point = Point("vix") \
                .tag("type",' history') \
                .field("open", float(ds[1])) \
                .field("high", float(ds[2])) \
                .field("low", float(ds[3])) \
                .field("close", float(ds[4])) \
                .time(datetime.strptime(ds[0],"%m/%d/%Y"),WritePrecision.NS)
                data.append(point)
                # influxdb批次寫入資料的效率較單資料點寫入高
            if (ind+1)%5000==0:
                write_api.write(bucket, org, data)
                data=[] # 清空
                print('write data to influxdb')
		        # 剩下的一次寫入<5000筆
            write_api.write(bucket, org, data)    
            
        except Exception as e:
            print(e)
# Dag名稱: vix_quoter
with DAG('vix_quoter', default_args=default_args,schedule_interval='@daily') as dag:
    vix_crawler = PythonOperator(
        task_id='vix_crawler',
        python_callable=vix_crawler,
        provide_context=True,
    )
    write_task = PythonOperator(
        task_id='write_task',
        python_callable=write_to_influxdb,
        provide_context=True,
    )
		# DAG串聯task
    vix_crawler >>write_task