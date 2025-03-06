import yfinance as yf
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import snowflake.connector
import requests
def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id = 'snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()
@task
def extract(ticker_symbols):
    data = {}
    for symbol in ticker_symbols:
        ticker = yf.Ticker(symbol)
        stock_data = ticker.history(period = '180d')
        data[symbol] = stock_data
    return data
@task
def transform(stock_data):
    records = []
    for symbol, data in stock_data.items():
        for date, row in data.iterrows():
            open = row["Open"]
            high = row["High"]
            low = row["Low"]
            close = row["Close"]
            volume = row["Volume"]
            records.append((symbol, date.date(), open, high, low, close, volume))
    return records
@task
def load(records, target_table):
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {target_table} (symbol VARCHAR, date DATE, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume FLOAT);")
        cur.execute(f"DELETE FROM {target_table};")
        for r in records:
            symbol = r[0].replace("'", "''")
            date = r[1]
            open = float(r[2])
            high = float(r[3])
            low = float(r[4])
            close = float(r[5])
            volume = float(r[6])
            sql = f"INSERT INTO {target_table} (symbol, date, open, high, low, close, volume) VALUES ('{symbol}', '{date}', {open}, {high}, {low}, {close}, {volume});"
            cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e
with DAG(
    dag_id = 'StockPrice_DataLoad_Pipeline',
    start_date = datetime(2025, 3, 1),
    catchup = False,
    tags = ['ETL'],
    schedule_interval = '0 7 * * *',
    default_args = {'retires': 3,
                    'retry_delay': timedelta(minutes=5)}
) as dag:
    target_table = "dev.stock_schema.stock_price"
    ticker_symbols = Variable.get("stock_symbols").split(",")
    stock_data = extract(ticker_symbols)
    records = transform(stock_data)
    load(records, target_table)