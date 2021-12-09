import json
from datetime import datetime

from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import requests


def get_travel_report_today():
    url = 'https://static.airasia.com/flights/station/v2/th-th/file.json'
    response = requests.get(url)
    dataset = response.json()
    #with open('data2.json', 'w') as f:
        #json.dump(data2, f)
    return dataset


def save_data_into_db():
    #mysql_hook = MySqlHook(mysql_conn_id='app_db')
    dataset = get_travel_report_today()
    for data in dataset:
        import mysql.connector
        db = mysql.connector.connect(host='54.91.39.146',user='root',passwd='Noom@dti',db='travel')

        cursor = db.cursor()
        AirportName = data['AirportName'].replace('\n',' ')
        AlternativeName = data['AlternativeName'].replace('\n',' ')
        CountryCode = data['CountryCode']
        CountryName = data['CountryName']
        StationCode = data['StationCode']
        StationName = data['StationName']
        StationType = data['StationType']
        AAFlight = data['AAFlight']

        cursor.execute('INSERT INTO travel_tb ( AirportName, AlternativeName, CountryCode , CountryName , StationCode, StationName, StationType, AAFlight)'
                  'VALUES("%s", "%s", "%s","%s", "%s", "%s", "%s","%s")',
                   (AirportName, AlternativeName, CountryCode,CountryName,StationCode,StationName,StationType,AAFlight))

        db.commit()
        print("Record inserted successfully into  table")
        cursor.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),

}
with DAG('travel_data_pipeline',
         schedule_interval='@daily',
         default_args=default_args,
         description='A simple data pipeline for travel report',
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='get_travel_report_today',
        python_callable= get_travel_report_today
    )

    t2 = PythonOperator(
        task_id='save_data_into_db',
        python_callable=save_data_into_db
    )

    t1 >> t2
