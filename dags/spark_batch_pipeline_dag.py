from datetime import time

import pendulum
from airflow import DAG
from airflow.sensors.time_sensor import TimeSensor
from spark_kubernetes.operators import spark_application_task

MARKET_TZ = pendulum.timezone("UTC")

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="spark_ohlcv_daily_pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz=MARKET_TZ),
    schedule="0 6 * * *",
    catchup=False,
    tags=["spark", "batch", "daily", "ohlcv"],
) as ohlcv_daily_dag:
    wait_0630 = TimeSensor(task_id="wait_until_0630", target_time=time(6, 30))
    wait_0700 = TimeSensor(task_id="wait_until_0700", target_time=time(7, 0))
    wait_0715 = TimeSensor(task_id="wait_until_0715", target_time=time(7, 15))
    wait_0730 = TimeSensor(task_id="wait_until_0730", target_time=time(7, 30))

    ohlcv_daily_loader = spark_application_task(
        "ohlcv-daily-loader-spark-application.yaml"
    )
    ohlcv_daily_cleaner = spark_application_task(
        "ohlcv-daily-cleaner-spark-application.yaml"
    )
    fact_ohlcv_daily_builder = spark_application_task(
        "fact-ohlcv-daily-builder-spark-application.yaml"
    )
    rule_engine_context_builder = spark_application_task(
        "rule-engine-context-builder-spark-application.yaml"
    )
    sync_custom_alerts = spark_application_task(
        "sync-custom-alerts-spark-application.yaml"
    )

    ohlcv_daily_loader >> wait_0630 >> ohlcv_daily_cleaner
    ohlcv_daily_cleaner >> wait_0700 >> fact_ohlcv_daily_builder
    fact_ohlcv_daily_builder >> wait_0715 >> rule_engine_context_builder
    rule_engine_context_builder >> wait_0730 >> sync_custom_alerts


with DAG(
    dag_id="spark_news_daily_pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz=MARKET_TZ),
    schedule="0 6 * * *",
    catchup=False,
    tags=["spark", "batch", "daily", "news"],
) as news_daily_dag:
    spark_application_task("news-cleaner-spark-application.yaml")


with DAG(
    dag_id="spark_batch_weekly_dimension_pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 4, tz=MARKET_TZ),
    schedule="0 5 * * 0",
    catchup=False,
    tags=["spark", "batch", "weekly", "dimension"],
) as weekly_dag:
    company_info_loader = spark_application_task(
        "company-info-loader-spark-application.yaml"
    )
    dim_loader = spark_application_task("dim-loader-spark-application.yaml")

    company_info_loader >> dim_loader
