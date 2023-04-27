from airflow.decorators import dag, task, task_group
from datetime import datetime
from include.dataset import DATASET_BTC
import json

@dag(start_date=datetime(2023, 4, 26), schedule=[DATASET_BTC], catchup=False)
def btc_process():

    @task
    def extract_currency():
        with open(DATASET_BTC.uri, 'r') as f:
            btc_data = json.load(f)
        print(btc_data)
        return [{key: value} for key, value in btc_data['bpi'].items()]

    @task_group
    def processing_btc(currency):

        @task
        def extract_rate(currency):
            return currency
        
        @task 
        def print_rate(currency):
            print(currency)

        print_rate(extract_rate(currency))

    processing_btc.expand(currency=extract_currency())

btc_process()
