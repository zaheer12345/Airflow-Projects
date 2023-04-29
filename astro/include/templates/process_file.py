from airflow import Dag
from airflow.decorators import task
from datetime import datetime


with Dag(f"process_DAG_ID_HOLDER", start_date=datetime(2023,4,29), schedule_interval="SCHEDULE_INTERVAL_HOLDER", Catchup=False) as dag:

        @task
        def extract(filename):
            return filename
        

        @task
        def process(filename):
            return filename
        
        @task
        def send_email(filename):
            print(filename)
            return filename
        
        send_email(process(extract(filename)))
    
   

