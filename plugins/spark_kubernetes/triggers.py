import asyncio
import logging
from typing import Any, Dict, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent
from kubernetes_asyncio import client as async_client
from kubernetes_asyncio import config as async_config

logger = logging.getLogger("airflow.task")


class SparkLifecycleTrigger(BaseTrigger):
    def __init__(self, name: str, namespace: str, poll_interval: int = 10):
        super().__init__()
        self.name = name
        self.namespace = namespace
        self.poll_interval = poll_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return (
            f"{self.__class__.__module__}.{self.__class__.__qualname__}",
            {
                "name": self.name,
                "namespace": self.namespace,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self):
        try:
            async_config.load_incluster_config()
        except Exception:
            await async_config.load_kube_config()

        async with async_client.ApiClient() as api_client:
            api = async_client.CustomObjectsApi(api_client)
            missing_id_retries = 0
            max_id_retries = 6

            while True:
                try:
                    resource = await api.get_namespaced_custom_object(
                        group="sparkoperator.k8s.io",
                        version="v1beta2",
                        namespace=self.namespace,
                        plural="sparkapplications",
                        name=self.name,
                    )
                    status = resource.get("status", {})
                    app_state = status.get("applicationState", {})
                    state = app_state.get("state", "UNKNOWN")
                    app_id = status.get("sparkApplicationId") or app_state.get(
                        "sparkApplicationId"
                    )

                    if state in ["COMPLETED", "SUCCEEDED"]:
                        if not app_id and missing_id_retries < max_id_retries:
                            missing_id_retries += 1
                            await asyncio.sleep(self.poll_interval)
                            continue
                        yield TriggerEvent(
                            {"status": "success", "spark_app_id": app_id}
                        )
                        return

                    if state in ["FAILED", "SUBMISSION_FAILED"]:
                        err = app_state.get("errorMessage", "Unknown K8s Error")
                        yield TriggerEvent(
                            {
                                "status": "failed",
                                "spark_app_id": app_id,
                                "message": f"{state}: {err}",
                            }
                        )
                        return

                    await asyncio.sleep(self.poll_interval)

                except async_client.ApiException as e:
                    if e.status == 404:
                        await asyncio.sleep(self.poll_interval)
                        continue
                    yield TriggerEvent({"status": "error", "message": str(e)})
                    return
                except Exception as e:
                    yield TriggerEvent({"status": "error", "message": str(e)})
                    return
