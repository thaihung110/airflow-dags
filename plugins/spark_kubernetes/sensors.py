import logging
from typing import Any, Dict

from airflow.exceptions import AirflowException
from airflow.models import BaseOperatorLink, XCom
from airflow.models.taskinstance import TaskInstanceKey
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from spark_kubernetes.triggers import SparkLifecycleTrigger

logger = logging.getLogger("airflow.task")

SPARK_HISTORY_HOST = "https://openhouse.spark-history.test"


class SparkHistoryLink(BaseOperatorLink):
    name = "Spark History"

    def get_link(self, operator, *, ti_key: TaskInstanceKey) -> str:
        try:
            spark_app_id = XCom.get_value(key="spark_app_id", ti_key=ti_key)
            if spark_app_id:
                return f"{SPARK_HISTORY_HOST}/history/{spark_app_id}"
        except Exception:
            pass
        return SPARK_HISTORY_HOST


class SparkLifecycleSensor(BaseSensorOperator):
    operator_extra_links = (SparkHistoryLink(),)
    template_fields = ("name", "namespace")

    def __init__(self, name: str, namespace: str, **kwargs):
        super().__init__(**kwargs)
        self.name = name
        self.namespace = namespace

    def execute(self, context: Context):
        self.defer(
            trigger=SparkLifecycleTrigger(
                name=self.name,
                namespace=self.namespace,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Dict[str, Any]):
        status = event.get("status")
        app_id = event.get("spark_app_id")
        msg = event.get("message", "")

        if app_id:
            context["ti"].xcom_push(key="spark_app_id", value=app_id)

        if status == "success":
            logger.info("Spark job succeeded. App ID: %s", app_id)
            return

        real_state, real_id = self._verify_status_sync()
        if real_state in ["COMPLETED", "SUCCEEDED"]:
            if real_id:
                context["ti"].xcom_push(key="spark_app_id", value=real_id)
            return

        raise AirflowException(
            f"Spark job failed. Final state: {real_state}. Details: {msg}"
        )

    def _verify_status_sync(self):
        try:
            hook = KubernetesHook(conn_id="kubernetes_default")
            crd = hook.get_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=self.namespace,
                plural="sparkapplications",
                name=self.name,
            )
            status = crd.get("status", {})
            state = status.get("applicationState", {}).get("state", "UNKNOWN")
            app_id = status.get("sparkApplicationId") or status.get(
                "applicationState", {}
            ).get("sparkApplicationId")
            return state, app_id
        except Exception as e:
            logger.error("Sync verification failed: %s", e)
            return "UNKNOWN", None
