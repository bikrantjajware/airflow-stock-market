from airflow.decorators import dag, task
from datetime import datetime



@dag(
        start_date=datetime(2026,1,1),
        schedule='@daily',
        catchup=False,
        tags=['stock_market']
)
def stock_market():

    @task
    def task1():
        pass


stock_market()