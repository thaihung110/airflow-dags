import copy
import logging
from pathlib import Path
from typing import Any, Dict

import yaml
from airflow.decorators import task
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.utils.task_group import TaskGroup
from spark_kubernetes.sensors import SparkLifecycleSensor

logger = logging.getLogger("airflow.task")

# plugins/spark_kubernetes/operators.py → parents[2] = repo root
# repo root contains spark-application/k8s/
SPARK_MANIFEST_DIR = (
    Path(__file__).resolve().parents[2] / "spark-application" / "k8s"
)


class DictSparkKubernetesOperator(SparkKubernetesOperator):
    def execute(self, context):
        if not isinstance(self.application_file, dict):
            return super().execute(context)

        body = self.application_file
        meta = body.get("metadata", {})
        name = meta.get("name")
        namespace = self.namespace or meta.get("namespace", "default")

        hook = KubernetesHook(conn_id=self.kubernetes_conn_id)
        logger.info(
            "Submitting SparkApplication %s in namespace %s", name, namespace
        )
        hook.create_custom_object(
            "sparkoperator.k8s.io",
            "v1beta2",
            "sparkapplications",
            body,
            namespace,
        )

        context["ti"].xcom_push(key="job_name", value=name)
        context["ti"].xcom_push(key="namespace", value=namespace)
        return {"job_name": name, "namespace": namespace}


@task
def load_spark_manifest(
    manifest_filename: str, run_suffix: str
) -> Dict[str, Any]:
    manifest_path = SPARK_MANIFEST_DIR / manifest_filename
    with manifest_path.open("r", encoding="utf-8") as manifest_file:
        manifest = yaml.safe_load(manifest_file)

    body = copy.deepcopy(manifest)
    metadata = body.setdefault("metadata", {})
    base_name = metadata["name"]
    metadata["name"] = f"{base_name}-{run_suffix}"[:63].rstrip("-")

    labels = metadata.setdefault("labels", {})
    labels["spark-app-template-name"] = base_name
    labels["airflow-managed"] = "true"

    return body


def delete_spark_job_on_failure(context):
    task_id = context["task"].task_id  # e.g. "ohlcv_daily_loader.monitor"
    group_prefix, _, _ = task_id.rpartition(".")
    submit_task_id = f"{group_prefix}.submit" if group_prefix else "submit"
    job_details = context["ti"].xcom_pull(
        task_ids=submit_task_id, key="return_value"
    )
    if not job_details:
        return

    name = job_details.get("job_name")
    namespace = job_details.get("namespace")
    try:
        hook = KubernetesHook(conn_id="kubernetes_default")
        hook.delete_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=namespace,
            plural="sparkapplications",
            name=name,
        )
        logger.info("Deleted SparkApplication after failure: %s", name)
    except Exception as e:
        logger.error("Failed to delete SparkApplication %s: %s", name, e)


def spark_application_task(manifest_filename: str) -> TaskGroup:
    """Build load → submit → monitor TaskGroup for one Spark manifest.

    Returning a TaskGroup instead of a bare task ensures that >> chaining
    between groups gates the entire load/submit/monitor sequence, not just
    the monitor task, so Spark apps are submitted strictly sequentially.
    """
    group_id = manifest_filename.removesuffix(
        "-spark-application.yaml"
    ).replace("-", "_")

    with TaskGroup(group_id=group_id) as group:
        manifest = load_spark_manifest.override(task_id="load_manifest")(
            manifest_filename=manifest_filename,
            run_suffix="{{ ts_nodash | lower }}",
        )

        submit = DictSparkKubernetesOperator(
            task_id="submit",
            kubernetes_conn_id="kubernetes_default",
            namespace="{{ ti.xcom_pull(task_ids='"
            + group_id
            + ".load_manifest')['metadata']['namespace'] }}",
            application_file=manifest,
            do_xcom_push=True,
        )

        monitor = SparkLifecycleSensor(
            task_id="monitor",
            name=submit.output["job_name"],
            namespace=submit.output["namespace"],
            on_failure_callback=delete_spark_job_on_failure,
        )

        manifest >> submit >> monitor

    return group
