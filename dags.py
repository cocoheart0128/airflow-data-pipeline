import requests
import pandas as pd
import yfinance as yf
import json
import boto3
from io import StringIO
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pytz

tz_info = pytz.timezone('Asia/Seoul')
now_date_kst = "{{ (execution_date + macros.timedelta(hours=9) + macros.timedelta(hours=24)).strftime('%Y-%m-%d') }}"
yest_date_kst = "{{ (execution_date + macros.timedelta(hours=9)).strftime('%Y-%m-%d') }}"

def upload_dataframe_to_s3(df, bucket, key):
    # Convert the DataFrame to a CSV string
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Initialize S3 client
    s3_client = boto3.client("s3")
    
    # Upload the CSV string to S3
    s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    print(f"DataFrame uploaded to s3://{bucket}/{key}")

def fetch_data_to_df():
    df = pd.DataFrame()
    stock_list = ['AAPL', 'TSLA']
    for stock_name in stock_list:
        stock = yf.Ticker(stock_name)
        data = stock.history(period="5d")
        data['comp'] = stock_name

        df = pd.concat([data, df], axis=0)
    return df

def get_data_and_upload(**kwargs):
    df = fetch_data_to_df()
    etl_date = kwargs['execution_date'] + timedelta(hours=9)  # Add 9 hours to match KST timezone
    formatted_etl_date = etl_date.strftime('%Y-%m-%d')  # Format it for use in S3 path
    upload_dataframe_to_s3(df, "data_bucket", f"stock/{formatted_etl_date}/stock.csv")

def get_delete_sql_query(**kwargs):
    # Get the formatted execution date in KST timezone
    now_date_kst = kwargs['execution_date'] + timedelta(hours=9)  # Adjust for KST timezone
    formatted_now_date_kst = now_date_kst.strftime('%Y-%m-%d')  # Format it for SQL query

    sql_query = f"DELETE FROM stock_table WHERE date <= '{formatted_now_date_kst}'"
    return sql_query


with DAG(
    dag_id="stock_daily_dag",
    start_date=datetime(2022, 1, 1, tzinfo=tz_info),
    schedule="@daily"
) as dag:

    dag_start = EmptyOperator(task_id='dag_start')
    dag_end = EmptyOperator(task_id='dag_end')
    
    t1 = PythonOperator(
        task_id="get_data_and_upload",
        python_callable=get_data_and_upload
    )

    t2 = GlueCrawlerOperator(
        task_id='run_glue_crawler',
        config={'Name': 'stock_crawler'},
        aws_conn_id='aws_default',  # Airflow에서 설정한 AWS 연결 ID
        wait_for_completion=True,  # Crawler 작업 완료 대기
    )

    t3 = SQLExecuteQueryOperator(
        task_id="delete_old_records_query",
        conn_id  = 'sql_conn',
        sql=get_delete_sql_query
    )

    t4 = SQLExecuteQueryOperator(
        task_id="insert_data",
        conn_id  = 'sql_conn', 
        sql = '''insert into stock_table SELECT * FROM crawler_table'''
    )

    dag_start >> t1 >> t2 >> t3 >> t4 >> dag_end
