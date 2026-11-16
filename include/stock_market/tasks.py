from airflow.sdk.bases.hook import BaseHook
from minio import Minio
from airflow.exceptions import AirflowNotFoundException
import boto3
import pandas as pd
from sqlalchemy import create_engine



BUCKET_NAME = 'stock-market'


def _get_minio_client():
    minio = BaseHook.get_connection('minio')
    print('minio extra', minio.extra_dejson)
    client = Minio(
            endpoint=minio.extra_dejson['endpoint'].split('//')[1],
            access_key=minio.login,
            secret_key=minio.password,
            secure=False
            )
    return client


def _get_stock_prices(url, symbol):
    import requests
    import json
    url = f"{url}{symbol}?metrics=high&interval=1d&range=1y"
    api = BaseHook.get_connection('stocks_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])
    data = response.json()
    print(f'stock {symbol}')
    print(f'result from get prices', data['chart']['result'][0])
    return json.dumps(data['chart']['result'][0])


def _store_stock_prices(stock):
    import json
    from io import BytesIO

    stock = json.loads(stock)
    client = _get_minio_client()

    if not client.bucket_exists(bucket_name=BUCKET_NAME):
        client.make_bucket(bucket_name=BUCKET_NAME)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name= f'{symbol}/prices.json',
        data=BytesIO(data),
        length=len(data)
        )
    return f'{BUCKET_NAME}/{symbol}'



def _get_formatted_csv(path):

    client = _get_minio_client()
    dir_path = f'{path.split('/')[1]}/formatted_prices/'
    objects = client.list_objects(bucket_name=BUCKET_NAME,prefix=dir_path, recursive=True)
    for obj in objects:
        if obj.object_name.endswith('csv'):
            return obj.object_name
    return AirflowNotFoundException(f'The csv does not exist at path {dir_path}')



def _load_to_dw(filename):
    minio = _get_minio_client()
    postgres = BaseHook.get_connection('postgres')

    obj = minio.get_object(
        bucket_name=BUCKET_NAME,
        object_name=filename
    )
    

    try:
        df = pd.read_csv(obj)
        engine = create_engine(postgres.get_uri())
        
        df.to_sql(
            name='stock_market',
            con=engine,
            schema='public',
            if_exists='append',
            index=False
        )
    except Exception as e:
        print('failed to read csv and load data in postgres', e)
    finally:
        obj.close()
        obj.release_conn()

    

    