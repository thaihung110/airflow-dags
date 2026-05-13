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

from spark_kubernetes.sensors import SparkLifecycleSensor

logger = logging.getLogger("airflow.task")

# plugins/spark_kubernetes/operators.py → parents[2] = repo root
# repo root contains spark-application/k8s/
SPARK_MANIFEST_DIR = Path(__file__).resolve().parents[2] / "spark-application" / "k8s"


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
    task_id = context["task"].task_id
    submit_task_id = task_id.replace("monitor_", "submit_", 1)
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


def spark_application_task(manifest_filename: str):
    """Build load → submit → monitor task group for one Spark manifest."""
    task_name = manifest_filename.removesuffix("-spark-application.yaml")
    task_name = task_name.replace("-", "_")

    manifest = load_spark_manifest.override(
        task_id=f"load_{task_name}_manifest"
    )(
        manifest_filename=manifest_filename,
        run_suffix="{{ ts_nodash | lower }}",
    )

    submit = DictSparkKubernetesOperator(
        task_id=f"submit_{task_name}",
        kubernetes_conn_id="kubernetes_default",
        namespace="{{ ti.xcom_pull(task_ids='load_"
        + task_name
        + "_manifest')['metadata']['namespace'] }}",
        application_file=manifest,
        do_xcom_push=True,
    )

    monitor = SparkLifecycleSensor(
        task_id=f"monitor_{task_name}",
        name=submit.output["job_name"],
        namespace=submit.output["namespace"],
        on_failure_callback=delete_spark_job_on_failure,
    )

    manifest >> submit >> monitor
    return monitor
