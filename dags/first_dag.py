import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests as req
import psycopg2 as pg2
import datetime as dt
from datetime import datetime, timedelta

dag = DAG(
    "hw_3_4",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    description="hw_3_4",
    schedule=timedelta(minutes=10),
    start_date=datetime(2023, 12, 4),
    catchup=False,
    tags=["pars money price"],
)

def get_money_price():

    # Исходные данные

    access_key = '1d91087e976c5909c8828c0e9d5aaeec'
    source = 'RUB'
    currencies = 'BTC, USD, EUR'
    currencies_list = currencies.replace(' ','').split(',')

    # Создание таблиц

    hook = PostgresHook(postgres_conn_id="user_pg")
    conn = hook.get_conn()
    cursor = conn.cursor()

    for cur in currencies_list:
        cursor.execute('CREATE TABLE IF NOT EXISTS {}('\
               'id SERIAL PRIMARY KEY,'\
               'quote_timestamp timestamp,'\
               'source VARCHAR(4),'\
               'currency VARCHAR(4),'\
               'rate float);'.format(source + cur)
              )

    conn.commit()
    cursor.close()
    conn.close()

    # Запрос к Web API

    url = f'http://api.exchangerate.host/live?access_key={access_key}&source={source}&currencies={currencies}'

    response = req.get(url)
    data = response.json()

    if data['success'] != True or response.status_code != 200:
        print(f"api.exchangerate.host error - response = {data['success']}")
        return
    else: print('api.exchangerate.host OK')

    # Наполнение таблиц

    hook = PostgresHook(postgres_conn_id="user_pg")
    conn = hook.get_conn()
    cursor = conn.cursor()

    for cur in currencies_list:

        cur_pair = source + cur
        rate = data['quotes'][cur_pair]
        if rate > 0: rate = 1.0/rate
        else: rate = 'NULL'
    
        insert_str = f"""INSERT INTO {cur_pair} (quote_timestamp,source,currency,rate) 
                        VALUES ('{dt.datetime.fromtimestamp(data['timestamp'])}',
                        '{source}','{cur}',{rate});""")

        cursor.execute(insert_str)

    conn.commit()
    cursor.close()
    conn.close()

t1 = BashOperator(
    task_id="print_a_message",
    bash_command="echo 'Good morning my diggers!'",
    dag=dag,
)

t2 = PythonOperator(
    task_id="get_a_money_price",
    provide_context=True,
    python_callable=get_quotes,
    dag=dag,
)

t1 >> t2