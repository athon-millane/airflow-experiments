# Copyright 2021, Sadana Labs Quantitative Research Group
# All rights reserved

"""
First attempt at monthly data pull.
Focus on idempotency.
"""

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from datetime import *

YEARS = ['2017', '2018', '2019', '2020', '2021']
INTERVALS = ["1m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1mo"]
DAILY_INTERVALS = ["1m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"]
MONTHS = list(range(1,13))
MAX_DAYS = 35
BASE_URL = 'https://data.binance.vision/'
START_DATE = date(int(YEARS[0]), MONTHS[0], 1)
END_DATE = datetime.date(datetime.now())

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def pull_month(ts, **kwargs):
    """
    Pull one month of data for all coins on Binance REST API and save to s3.
    """


def csv_to_parquet(ts, **kwargs):
    """
    Pull csv from s3 into pandas and convert to parquet.
    Write back out to new location in s3.
    """


with DAG('pull_month_binance',
    start_date=START_DATE,
    max_active_runs=3,
    schedule_interval=timedelta(months=1),
    default_args=default_args,
    catchup=True
    ) as dag:

    t0 = DummyOperator(
        task_id='start'
    )

    tn = PythonOperator(
        task_id="python_pull_month",
        python_callable=pull_month,  # make sure you don't include the () of the function
        op_kwargs={'task_number': task},
    )

    task_0 >> task_1