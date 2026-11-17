from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.discord.notifications.discord import DiscordNotifier

from datetime import datetime


from include.stock_market.tasks import _get_stock_prices, _store_stock_prices, _get_formatted_csv, _load_to_dw

SYMBOL = 'NVDA'

@dag(
        start_date=datetime(2026,1,1),
        schedule='@daily',
        catchup=False,
        tags=['stock_market'],
        on_success_callback=DiscordNotifier(
                discord_conn_id= 'discord_alert',
                text='stock_market Dag run is successful',
                username='',
                tts=True
        ),
        on_failure_callback=DiscordNotifier(
                discord_conn_id= 'discord_alert',
                text='stock_market Dag run has failed',
                username='',
                tts=True
        )
)
def stock_market():
        
        @task.sensor(poke_interval=30, timeout=300, mode='poke')
        def is_finance_api_available() -> PokeReturnValue:
                import requests
                api = BaseHook.get_connection('stocks_api')
                url = f"{api.host}/{api.extra_dejson['endpoint']}"
                print(url)

                response = requests.get(url, headers=api.extra_dejson['headers'])

                data = response.json()
                print('1st task result',data)
                result = data['finance']['result'] is None
                return PokeReturnValue(is_done=result, xcom_value=url)

        get_stock_prices = PythonOperator(
                task_id='get_stock_prices',
                python_callable=_get_stock_prices,
                op_kwargs={ 
                        'url': '{{ ti.xcom_pull(task_ids="is_finance_api_available") }}',
                        'symbol': SYMBOL
                        }
                )

        store_stock_prices = PythonOperator(
                task_id='store_stock_prices',
                python_callable=_store_stock_prices,
                op_kwargs={ 'stock': '{{ ti.xcom_pull(task_ids="get_stock_prices") }}' }
        )

        format_prices = DockerOperator(
                task_id='format_prices',
                image='airflow/stock-app',
                container_name='format_prices',
                api_version='auto',
                auto_remove='success',
                docker_url='tcp://docker-proxy:2375',
                network_mode='container:spark-master',
                tty=True,
                xcom_all=False,
                mount_tmp_dir=False,
                environment={
                        'SPARK_APPLICATION_ARGS': '{{ ti.xcom_pull(task_ids="store_stock_prices") }}'
                }
        )


        get_formatted_csv = PythonOperator(
                task_id='get_formatted_csv',
                python_callable=_get_formatted_csv,
                op_kwargs={ 'path': '{{ ti.xcom_pull(task_ids="store_stock_prices") }}' }
        )

        load_to_dw = PythonOperator(
                task_id='load_to_dw',
                python_callable=_load_to_dw,
                op_kwargs={ 'filename': '{{ ti.xcom_pull(task_ids="get_formatted_csv") }}' }
        )

        #archived operator, issue running the operator, caused conflict of numpy/python/airflow versions
        # load_to_dw = aql.load_file(
        #         task_id='load_to_dw',
        #         input_file=File(
        #                 path=f's3://{BUCKET_NAME}/{{{{ ti.xcom_pull(task_ids="get_formatted_csv") }}}}',
        #                 conn_id='minio'
        #         ),
        #         output_table=Table(
        #                 name='stock_market',
        #                 conn_id='postgres',
        #                 metadata=Metadata(schema='public')
        #         ),
        #         load_options={
        #                 'aws_access_key_id':BaseHook.get_connection('minio').login,
        #                 'aws_secret_access_key':BaseHook.get_connection('minio').password,
        #                 'endpoint_url': BaseHook.get_connection('minio').host
        #         }
        # )


        is_finance_api_available() >> get_stock_prices >> store_stock_prices >> format_prices >> get_formatted_csv >> load_to_dw
        



                

                
        


stock_market()