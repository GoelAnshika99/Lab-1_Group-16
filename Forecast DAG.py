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
def train_model(symbols, target_table):
    cur = return_snowflake_conn()
    try:
        cur.execute("""BEGIN;""")
        cur.execute("""DROP VIEW IF EXISTS STOCKS_ACTUAL_AND_FORECAST;""")
        for symbol in symbols:
            cur.execute(f"""DROP VIEW IF EXISTS STOCK_PRICE_FORECAST_{symbol};""")
            cur.execute(f"""CREATE OR REPLACE VIEW STOCK_PRICE_FORECAST_{symbol} AS
                        WITH feature_stock AS (
                        SELECT TO_TIMESTAMP_NTZ(TO_DATE(date)) AS STOCK_TS,
                        CLOSE
                        FROM {target_table} WHERE symbol = '{symbol}')
                        SELECT stock_ts, close from feature_stock order by stock_ts;""")
            cur.execute(f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST stock_mdl_{symbol}(
                        INPUT_DATA => TABLE(STOCK_PRICE_FORECAST_{symbol}),
                        TIMESTAMP_COLNAME => 'STOCK_TS',
                        TARGET_COLNAME => 'close',
                        CONFIG_OBJECT => {{'ON_ERROR': 'SKIP'}});""")
        cur.execute("""COMMIT;""")  # Commit transaction after all forecasts
    except Exception as e:
        cur.execute("ROLLBACK;")  # Rollback on any error
        print(f"Error during forecasting process: {e}")
        raise e
@task
def predict(symbols, target_table):
    cur = return_snowflake_conn()
    try:
        cur.execute("""BEGIN;""")
        for symbol in symbols:
            cur.execute(f"""CALL stock_mdl_{symbol}!FORECAST(FORECASTING_PERIODS => 7);""")
            cur.execute(f"""CREATE OR REPLACE TABLE STOCKS_FORECASTED_CLOSE_{symbol} AS SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));""")
            cur.execute(f"""CREATE OR REPLACE TABLE COMBINED_DATA_{symbol} AS
                        SELECT symbol, TO_TIMESTAMP_NTZ(TO_DATE(date)) as stock_ts, open, high, low, volume, 
                        close as ACTUAL_CLOSE, NULL AS FORECAST_CLOSE, NULL AS FORECAST_CLOSE_LOWER_BOUND, NULL AS FORECAST_CLOSE_UPPER_BOUND
                        FROM {target_table} WHERE symbol = '{symbol}'
                        UNION ALL
                        SELECT '{symbol}' AS SYMBOL, TS AS STOCK_TS, NULL AS OPEN, NULL AS HIGH, NULL AS LOW, 
                        NULL AS VOLUME, NULL AS ACTUAL_CLOSE, forecast as FORECAST_CLOSE, LOWER_BOUND AS FORECAST_CLOSE_LOWER_BOUND, UPPER_BOUND AS FORECAST_CLOSE_UPPER_BOUND
                        FROM STOCKS_FORECASTED_CLOSE_{symbol};""")
            cur.execute("""CREATE OR REPLACE TABLE STOCK_PRICE_FORECAST AS
                    SELECT * FROM COMBINED_DATA_AAPL
                    UNION ALL
                    SELECT * FROM COMBINED_DATA_GOOG
                    UNION ALL
                    SELECT * FROM COMBINED_DATA_MSFT;""")
        cur.execute("""COMMIT;""")  # Commit transaction after all forecasts
    except Exception as e:
        cur.execute("ROLLBACK;")  # Rollback on any error
        print(f"Error during forecasting process: {e}")
        raise e
with DAG(
    dag_id = 'StockPrice_Forecast_Pipeline',
    start_date = datetime(2025, 3, 1),
    catchup = False,
    tags = ['ETL'],
    schedule_interval = '30 7 * * *'
) as dag:
    target_table = "dev.stock_schema.stock_price"
    ticker_symbols = Variable.get("stock_symbols").split(",")
    train_model(ticker_symbols, target_table)>>predict(ticker_symbols, target_table)