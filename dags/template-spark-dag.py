import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import jinja2
import yaml

# Airflow Core
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models import BaseOperatorLink, XCom
from airflow.models.param import Param
from airflow.models.taskinstance import TaskInstanceKey
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook

# Kubernetes Provider
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.context import Context
from kubernetes_asyncio import client as async_client
from kubernetes_asyncio import config as async_config

# Initialize Standard Logger (Fixes 'RuntimeTaskInstance has no log' error)
logger = logging.getLogger("airflow.task")

# ==============================================================================
# 1. SPARK HISTORY LINK
# ==============================================================================


class SparkHistoryLink(BaseOperatorLink):
    name = "Spark History"

    def get_link(self, operator, *, ti_key: TaskInstanceKey) -> str:
        HISTORY_HOST = "https://spark-history.dmp.demo"
        try:
            # Safe fetch for Airflow 3.0 XComs
            spark_app_id = XCom.get_value(key="spark_app_id", ti_key=ti_key)
            if spark_app_id:
                return f"{HISTORY_HOST}/history/{spark_app_id}"
        except Exception:
            pass
        return HISTORY_HOST


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
            await async_config.load_incluster_config()
        except:
            await async_config.load_kube_config()

        async with async_client.ApiClient() as api_client:
            api = async_client.CustomObjectsApi(api_client)
            missing_id_retries = 0
            MAX_ID_RETRIES = 6

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
                        if not app_id and missing_id_retries < MAX_ID_RETRIES:
                            missing_id_retries += 1
                            await asyncio.sleep(self.poll_interval)
                            continue
                        yield TriggerEvent(
                            {"status": "success", "spark_app_id": app_id}
                        )
                        return

                    elif state in ["FAILED", "SUBMISSION_FAILED"]:
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
                    else:
                        yield TriggerEvent(
                            {"status": "error", "message": str(e)}
                        )
                        return
                except Exception as e:
                    yield TriggerEvent({"status": "error", "message": str(e)})
                    return


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
                name=self.name, namespace=self.namespace
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
            logger.info(f"✅ Spark Job Succeeded. App ID: {app_id}")
            return

        logger.warning(
            f"Trigger reported: {status}. Verifying via Worker API..."
        )
        real_state, real_id = self._verify_status_sync()

        if real_state in ["COMPLETED", "SUCCEEDED"]:
            logger.info(
                f"✅ Verified Success via Worker API. App ID: {real_id}"
            )
            if real_id:
                context["ti"].xcom_push(key="spark_app_id", value=real_id)
            return

        raise AirflowException(
            f"Spark Job Failed. Final State: {real_state}. Details: {msg}"
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
            logger.error(f"Sync Verification Failed: {e}")
            return "UNKNOWN", None


class DictSparkKubernetesOperator(SparkKubernetesOperator):
    template_fields = list(SparkKubernetesOperator.template_fields) + [
        "dry_run"
    ]

    def __init__(self, dry_run=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dry_run = dry_run

    def execute(self, context):
        if isinstance(self.application_file, dict):
            body = self.application_file
            meta = body.get("metadata", {})
            name = meta.get("name")
            ns = self.namespace or meta.get("namespace", "default")

            if str(self.dry_run).lower() in ["true", "1", "yes"]:
                return {"job_name": name, "namespace": ns}

            hook = KubernetesHook(conn_id=self.kubernetes_conn_id)
            logger.info(f"Submitting SparkApplication: {name}")
            hook.create_custom_object(
                "sparkoperator.k8s.io", "v1beta2", "sparkapplications", body, ns
            )

            # FIX: Explicitly push keys to XCom
            context["ti"].xcom_push(key="job_name", value=name)
            context["ti"].xcom_push(key="namespace", value=ns)

            return {"job_name": name, "namespace": ns}
        else:
            return super().execute(context)


# ==============================================================================
# SECTION B: CALLBACKS & DAG
# ==============================================================================


def delete_spark_job_on_failure(context):
    """
    Callback running on Worker. Fixes 'no attribute log' error.
    """
    ti = context["ti"]

    # 1. Retrieve Job Details using standard XCom pull
    # Note: We pull the return_value (which is a dict)
    job_details = ti.xcom_pull(task_ids="start_spark_job", key="return_value")

    if not job_details:
        logger.warning("⚠️ No job details in XCom. Job might not have started.")
        return

    name = job_details.get("job_name")
    namespace = job_details.get("namespace")

    logger.info(f"🛑 RECEIVED STOP SIGNAL. Deleting Spark Job: {name}...")

    try:
        hook = KubernetesHook(conn_id="kubernetes_default")
        hook.delete_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=namespace,
            plural="sparkapplications",
            name=name,
        )
        logger.info(f"✅ Spark Job Deleted: {name}")
    except Exception as e:
        logger.error(f"❌ Failed to delete job: {e}")


# ==============================================================================
# 1. PLATFORM SETTINGS (Hidden from User)
# ==============================================================================
# These are controlled by the Platform Team, not the End User.
PLATFORM_NAMESPACE = "dmp-lakehouse-demo"
PLATFORM_SERVICE_ACCOUNT = "spark-operator-spark"

# ==============================================================================
# 3. JINJA2 TEMPLATE (Secure & Polyglot)
# ==============================================================================
SPARK_YAML_TEMPLATE = """
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
    name: "{{ job_name }}"
    namespace: "{{ tenant_namespace }}"
    spec:
    # Dynamic Type: "Python" or "Scala"
    type: {{ app_type }}
    
    mode: cluster
    image: "{{ image_repo }}:{{ image_tag }}"
    imagePullPolicy: IfNotPresent
    sparkVersion: "3.5.0"
    
    # Path to main file
    mainApplicationFile: "local://{{ main_file_path }}"

    # Logic: Only inject mainClass if it is provided (Java/Scala)
    {% if main_class %}
    mainClass: "{{ main_class }}"
    {% endif %}

    # Logic: Only inject pythonVersion if it is a Python job
    {% if app_type == 'Python' %}
    pythonVersion: "3"
    {% endif %}

    restartPolicy:
        type: Never

    # ----------------------------------------------------------------------------
    # Driver Configuration
    # ----------------------------------------------------------------------------
    driver:
        cores: {{ driver_cores }}
        coreLimit: "{{ driver_cores }}200m"
        memory: "{{ driver_memory }}"
        serviceAccount: "{{ tenant_sa }}"
        labels:
        version: 3.5.0
        
        # Conditional Volume Mount: Only if config path is provided
        {% if s3_config_path %}
        volumeMounts:
        - name: config-ramdisk
            mountPath: /etc/secrets
            readOnly: true
        {% endif %}
        
        env:
        - name: ENVIRONMENT
            value: "PROD"
        - name: AWS_ACCESS_KEY_ID
            valueFrom:
            secretKeyRef:
                name: minio-root-credentials
                key: rootUser
        - name: AWS_SECRET_ACCESS_KEY
            valueFrom:
            secretKeyRef:
                name: minio-root-credentials
                key: rootPassword

        # Conditional Env Var: Only if config path is provided
        {% if s3_config_path %}
        - name: APP_CONFIG_PATH
            value: "/etc/secrets/app_config.json"
        {% endif %}

        # User-defined Env Vars
        {% if user_env_vars %}
        {% for key, value in user_env_vars.items() %}
        - name: {{ key }}
            value: "{{ value }}"
        {% endfor %}
        {% endif %}
    
    # Platform Secrets (Always injected for InitContainers or internal use)
    # envFrom:
    #   - secretRef:
    #       name: platform-secrets 

    # Conditional InitContainer: Only run if we need to fetch config
    {% if s3_config_path %}
    initContainers:
    - name: fetch-config
        image: minio/mc:latest
        imagePullPolicy: IfNotPresent
        command: ["/bin/sh", "-c"]
        args: 
            - >-
            mc alias set minio $S3_ENDPOINT $S3_ACCESS_KEY $S3_SECRET_KEY;
            echo "Fetching config from {{ s3_config_path }}";
            mc cp {{ s3_config_path }} /mnt/ramdisk/app_config.json;
        volumeMounts:
            - name: config-ramdisk
            mountPath: /mnt/ramdisk
        envFrom:
            - secretRef:
                name: platform-secrets
        {% endif %}

  # ----------------------------------------------------------------------------
  # Executor Configuration
  # ----------------------------------------------------------------------------
  executor:
    cores: {{ executor_cores }}
    instances: {{ executor_instances }}
    memory: "{{ executor_memory }}"
    # Conditional Volume Mount
    {% if s3_config_path %}
    volumeMounts:
      - name: config-ramdisk
        mountPath: /etc/secrets
        readOnly: true
    {% endif %}

  # ----------------------------------------------------------------------------
  # Conditional Volumes Definition
  # ----------------------------------------------------------------------------
  {% if s3_config_path %}
  volumes:
    - name: config-ramdisk
      emptyDir:
        medium: "Memory"
        sizeLimit: "10Mi"
  {% endif %}

  # ----------------------------------------------------------------------------
  # Application Arguments
  # ----------------------------------------------------------------------------
  {% if app_arguments %}
  arguments:
    {% for arg in app_arguments %}
    - "{{ arg }}"
    {% endfor %}
  {% endif %}
  # ----------------------------------------------------------------------------
  # Spark Configuration
  # ----------------------------------------------------------------------------
  sparkConf:
    # "spark.ui.port": "4040"
    "spark.eventLog.enabled": "true"
    "spark.eventLog.dir": "s3a://spark-events/logs/"
    spark.history.fs.inProgressOptimization.enabled: 'true'
    spark.history.fs.update.interval: '10s'
    "spark.history.fs.logDirectory": "s3a://spark-events/logs/"

    # "spark.driver.defaultJavaOptions": "-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
    
    # Critical for MinIO
    spark.hadoop.fs.s3a.endpoint: "http://storage-minio:9000"
    spark.hadoop.fs.s3a.path.style.access: "true" 
    spark.hadoop.fs.s3a.connection.ssl.enabled: "false" # If no HTTPS
    spark.hadoop.fs.s3a.fast.upload: "true"
    # not delete executor pod when termination for further getting error log
    "spark.kubernetes.executor.deleteOnTermination": "false"
    {% if spark_conf %}
    {% for key, value in spark_conf.items() %}
    "{{ key }}": "{{ value }}"
    {% endfor %}
    {% endif %}
"""

# ==============================================================================
# 4. AIRFLOW 3.0 DAG DEFINITION
# ==============================================================================
default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 0,
}


with DAG(
    dag_id="spark_job_template",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,  # <--- Airflow 3.0 Syntax (was schedule_interval)
    catchup=False,
    tags=["spark", "jobs", "template"],
    # --------------------------------------------------------------------------
    # User-Facing Params (No Namespace/SA here!)
    # --------------------------------------------------------------------------
    params={
        "dry_run": Param(
            False,
            type="boolean",
            description="Generate YAML logs but skip K8s submission.",
        ),
        "job_name_prefix": Param("prod-job", type="string"),
        "image_repo": Param("my-registry/spark-app", type="string"),
        "image_tag": Param("latest", type="string"),
        "main_file_path": Param("/opt/application/main.py", type="string"),
        "main_class": Param("", type=["string", "null"]),
        "s3_config_path": Param(
            "s3/configs/prod-config.json", type=["string", "null"]
        ),
        "driver_cores": Param(1, type="integer"),
        "driver_memory": Param("1g", type="string"),
        "executor_cores": Param(1, type="integer"),
        "executor_memory": Param("2g", type="string"),
        "executor_instances": Param(2, type="integer"),
        "user_env_vars": Param({"MY_VAR": "value"}, type=["object", "null"]),
        "app_arguments": Param(["--verbose"], type=["array", "null"]),
        "spark_conf": Param(
            {"spark.sql.shuffle.partitions": "200"}, type=["object", "null"]
        ),
    },
) as dag:

    # --------------------------------------------------------------------------
    # Task 1: Render Manifest (Using Platform Constants)
    # --------------------------------------------------------------------------
    @task
    def render_manifest(**context):
        params = context["params"]

        # 1. Logic: Detect Type (Python/Scala)
        main_file = params["main_file_path"]
        main_class = params.get("main_class", "")

        if main_file.endswith(".py"):
            app_type = "Python"
            main_class = None
        elif main_file.endswith(".jar"):
            app_type = "Scala"
            if not main_class:
                raise ValueError("main_class is REQUIRED for Java/Scala")
        else:
            raise ValueError(f"Unknown extension: {main_file}")

        # 2. Logic: Handle Empty Config
        s3_path = params.get("s3_config_path")
        if s3_path and s3_path.strip() == "":
            s3_path = None

        job_name = f"{params['job_name_prefix']}-{context['ts_nodash'].lower()}"

        # 3. Render Template
        # CRITICAL: We pass the Platform Constants here
        template = jinja2.Template(SPARK_YAML_TEMPLATE)
        rendered_yaml_str = template.render(
            job_name=job_name,
            tenant_namespace=PLATFORM_NAMESPACE,  # <--- From Python Config
            tenant_sa=PLATFORM_SERVICE_ACCOUNT,  # <--- From Python Config
            app_type=app_type,
            main_class=main_class,
            image_repo=params["image_repo"],
            image_tag=params["image_tag"],
            main_file_path=params["main_file_path"],
            driver_cores=params["driver_cores"],
            driver_memory=params["driver_memory"],
            executor_cores=params["executor_cores"],
            executor_memory=params["executor_memory"],
            executor_instances=params["executor_instances"],
            user_env_vars=params.get("user_env_vars"),
            s3_config_path=s3_path,
            app_arguments=params.get("app_arguments"),
            spark_conf=params.get("spark_conf"),
        )

        # 4. Parse to Dict
        manifest_dict = yaml.safe_load(rendered_yaml_str)

        # --- DEBUG PREVIEW ---
        print("\n" + "=" * 50)
        print(f" [DEBUG] MANIFEST PREVIEW (Namespace: {PLATFORM_NAMESPACE})")
        print("=" * 50)
        print(json.dumps(manifest_dict, indent=2))
        print("=" * 50 + "\n")
        # ---------------------

        return manifest_dict

    # --------------------------------------------------------------------------
    # Task 2: Submit Job (Using Platform Constants)
    # --------------------------------------------------------------------------

    # 1. Generate
    spark_manifest = render_manifest()

    # 2. Submit
    submit_job = DictSparkKubernetesOperator(
        task_id="submit_spark_job",
        kubernetes_conn_id="kubernetes_default",
        namespace=PLATFORM_NAMESPACE,
        application_file=spark_manifest,
        dry_run="{{ params.dry_run }}",
        do_xcom_push=True,
    )

    # 3. Wait for Job (Async Sensor)
    # This task runs on the Triggerer service, consuming nearly ZERO resources.
    monitor_job = SparkLifecycleSensor(
        task_id="monitor_job_status",
        name=submit_job.output["job_name"],
        namespace=PLATFORM_NAMESPACE,
        on_failure_callback=delete_spark_job_on_failure,
    )

    spark_manifest >> submit_job >> monitor_job
