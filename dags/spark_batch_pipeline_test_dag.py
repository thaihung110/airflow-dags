import pendulum
from airflow import DAG
from spark_kubernetes.operators import spark_application_task

MARKET_TZ = pendulum.timezone("UTC")

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 0,
}

# ---------------------------------------------------------------------------
# DAG 1 — OHLCV daily pipeline (test)
#
# Production order: loader → cleaner → fact_builder → rule_engine_context_builder
#                   → sync_custom_alerts
# TimeSensors removed. rule_engine_context_builder commented out for partial testing.
#
# PREREQUISITE: spark_batch_weekly_dimension_pipeline_test must have run successfully
# at least once before uncommenting rule_engine_context_builder. dim_symbol must be
# populated or rule_engine_context_builder will fail with a clear error message.
#
# sync_custom_alerts PREREQUISITE: PostgreSQL user_alert_events table must exist and
# the rule-engine must have written at least one event row, or the job will succeed
# with 0 rows synced (watermark stays at epoch — safe to run even on empty table).
# ---------------------------------------------------------------------------

with DAG(
    dag_id="spark_ohlcv_daily_pipeline_test",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz=MARKET_TZ),
    schedule=None,
    catchup=False,
    tags=["spark", "batch", "daily", "ohlcv", "test"],
) as ohlcv_daily_test_dag:
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

    (
        ohlcv_daily_loader
        >> ohlcv_daily_cleaner
        >> fact_ohlcv_daily_builder
        >> rule_engine_context_builder
        >> sync_custom_alerts
    )

# ---------------------------------------------------------------------------
# DAG 2 — News daily pipeline (test)
# ---------------------------------------------------------------------------

with DAG(
    dag_id="spark_news_daily_pipeline_test",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz=MARKET_TZ),
    schedule=None,
    catchup=False,
    tags=["spark", "batch", "daily", "news", "test"],
) as news_daily_test_dag:
    spark_application_task("news-cleaner-spark-application.yaml")

# ---------------------------------------------------------------------------
# DAG 3 — Weekly dimension pipeline (test)
#
# Production order: company_info_loader → dim_loader
# ---------------------------------------------------------------------------

with DAG(
    dag_id="spark_batch_weekly_dimension_pipeline_test",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 4, tz=MARKET_TZ),
    schedule=None,
    catchup=False,
    tags=["spark", "batch", "weekly", "dimension", "test"],
) as weekly_test_dag:
    company_info_loader = spark_application_task(
        "company-info-loader-spark-application.yaml"
    )
    dim_loader = spark_application_task("dim-loader-spark-application.yaml")

    company_info_loader >> dim_loader
