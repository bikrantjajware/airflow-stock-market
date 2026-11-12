from airflow.sdk.bases.hook import BaseHook
from minio import Minio


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
    bucket_name = 'stock-market'
    minio = BaseHook.get_connection('minio')
    print('minio extra', minio.extra_dejson)
    client = Minio(
        endpoint=minio.extra_dejson['endpoint'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
        )

    if not client.bucket_exists(bucket_name=bucket_name):
        client.make_bucket(bucket_name=bucket_name)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')
    objw = client.put_object(
        bucket_name=bucket_name,
        object_name= f'{symbol}/prices.json',
        data=BytesIO(data),
        length=len(data)
        )
    return f'{bucket_name}/{symbol}'


