from airflow import DAG
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime,timedelta


default_args = {
    'owner': 'Wen',
    'start_date':datetime(2022, 10, 6, 0, 0),
    }

def finlab_data_getter(**context):
    """
    因為只會計算最多到ma20,為了降低後續的計算量,只取50筆即可
    """
    import finlab
    from finlab import data
    latest_data = 50 # 最新50筆
    finlab_token = Variable.get("finlab_token")
    finlab.login(api_token=finlab_token)  
    with data.universe('OTC'):
        close= data.get('price:收盤價')
        vol = data.get("price:成交股數")
    return close[-latest_data:],vol[-latest_data:]

def update_candidate(**context):

    close,vol = context['task_instance'].xcom_pull(task_ids='finlab_data_getter')
    ma5 = close.rolling(5).mean()
    ma10 = close.rolling(10).mean()
    ma20 = close.rolling(20).mean()
    vol_ma = vol.average(5)
    cond1 =(ma5>=ma10) & (ma10>=ma20) 
    cond2 =(ma5<=ma10)& (ma10<=ma20)
    cond3 = vol_ma > 200*1000
    candidate_long = cond1 & cond3
    candidate_short = cond2 & cond3
    return candidate_long,candidate_short

def write_to_influxdb(**context):
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS
    url = Variable.get("influxdb_url")
    token = Variable.get("token")
    org = Variable.get("org")
		# 透過xcom從vix_crawler取得df
    candidate_long,candidate_short = context['task_instance'].xcom_pull(task_ids='update_candidate')
    with InfluxDBClient(url, token=token, org=org) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        candidate_filter  = candidate_long
        try:
            for i in range(len(candidate_filter.index)):
                data=[]
                candidate_daily = candidate_filter.iloc[i]
                for j in range(len(candidate_daily.index)):
                    code = candidate_daily.index[j]
                    if candidate_daily[j]==True:
                        point = Point("indicator_ma_candidate")\
                        .tag("code",code)\
                        .field("position",'long')\
                        .field("market","OTC")\
                        .time(candidate_filter.index[i].strftime("%Y-%m-%d"),WritePrecision.S)
                        data.append(point)
                write_api.write("stock", org, data)
        except Exception as e:
            print(e)
        candidate_filter  = candidate_short
        try:
            for i in range(len(candidate_filter.index)):
                data=[]
                candidate_daily = candidate_filter.iloc[i]
                for j in range(len(candidate_daily.index)):
                    code = candidate_daily.index[j]
                    if candidate_daily[j]==True:
                        point = Point("indicator_ma_candidate")\
                        .tag("code",code)\
                        .field("position",'short')\
                        .field("market","OTC")\
                        .time(candidate_filter.index[i].strftime("%Y-%m-%d"),WritePrecision.S)
                        data.append(point)
                write_api.write("stock", org, data)
        except Exception as e:
            print(e)


with DAG('candidate_ma_source_finlab_otc', default_args=default_args,schedule_interval='10 12 * * *') as dag:
    """
    執行時間為每日晚上20:10
    """
    finlab_data_getter = PythonOperator(
        task_id='finlab_data_getter',
        python_callable=finlab_data_getter,
        provide_context=True,
    )
    update_candidate = PythonOperator(
        task_id='update_candidate',
        python_callable=update_candidate,
        provide_context=True,
    )

    write_to_influxdb = PythonOperator(
        task_id='write_to_influxdb',
        python_callable=write_to_influxdb,
        provide_context=True,
    )  
    # define workflow
    finlab_data_getter >> update_candidate >> write_to_influxdb



