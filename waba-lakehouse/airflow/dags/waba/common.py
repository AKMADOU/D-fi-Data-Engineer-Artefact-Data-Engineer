"""Briques communes aux DAGs WABA.

* `SparkJobOperator` : lance un job PySpark dans un conteneur éphémère (image Spark du
  projet) connecté au cluster Spark standalone. Le driver tourne dans ce conteneur,
  les executors sur les workers. Les identifiants ne sont jamais dans le code : ils
  sont lus au moment de l'exécution dans la Connection Airflow `minio_s3` et la
  Variable `pii_hash_secret`, puis injectés en variables d'environnement privées
  (non affichées dans l'UI ni dans les logs).
* Paramétrage par pays : `dag_run.conf["countries"]` > Variable `waba_countries` > 8 pays.
* Retries avec backoff exponentiel et alerte en cas d'échec (log structuré +
  webhook optionnel, compatible Slack / Teams, via la Variable `alert_webhook_url`).
* Level 4 (Kubernetes) : `WABA_SPARK_BACKEND=k8s` -> les jobs sont soumis au
  Spark Operator (ressource SparkApplication) ; les identifiants sont injectés
  depuis un Secret Kubernetes, jamais écrits dans la ressource. Les dates du dernier
  succès / échec de chaque DAG sont poussées au Prometheus Pushgateway
  (`WABA_PUSHGATEWAY_URL`) pour l'alerte Grafana du reporting réglementaire.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import urllib.request
from datetime import datetime, timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Asset, BaseHook, BaseOperator, Param, Variable, task

log = logging.getLogger("waba")

COUNTRIES = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"]
EVENT_DATASETS = ["bank_transactions", "insurance_operations", "mobile_money_payments",
                  "loan_repayments"]

# Assets (data-aware scheduling) : chaque couche publiée déclenche la suivante.
BRONZE = Asset("s3://lakehouse/bronze", extra={"layer": "bronze"})
SILVER = Asset("s3://lakehouse/silver", extra={"layer": "silver"})
GOLD = Asset("s3://lakehouse/gold", extra={"layer": "gold"})

# Infrastructure (non sensible) : variables d'environnement du déploiement.
SPARK_IMAGE = os.environ.get("WABA_SPARK_IMAGE", "waba/spark:3.5.3-iceberg1.6.1")
DOCKER_URL = os.environ.get("WABA_DOCKER_URL", "tcp://docker-proxy:2375")
SPARK_NETWORK = os.environ.get("WABA_SPARK_NETWORK", "waba_waba")
S3_CONN_ID = "minio_s3"
# Level 4 : "k8s" = Spark Operator ; "docker" (défaut) = conteneur éphémère (docker compose)
SPARK_BACKEND = os.environ.get("WABA_SPARK_BACKEND", "docker")
SPARK_NAMESPACE = os.environ.get("WABA_SPARK_NAMESPACE", "processing")
SPARK_SERVICE_ACCOUNT = os.environ.get("WABA_SPARK_SERVICE_ACCOUNT", "spark")
SPARK_ENV_SECRET = os.environ.get("WABA_SPARK_ENV_SECRET", "waba-spark-env")
PUSHGATEWAY_URL = os.environ.get("WABA_PUSHGATEWAY_URL", "")
START_DATE = datetime(2026, 1, 1)

COUNTRIES_XCOM = "{{ ti.xcom_pull(task_ids='resolve_countries') | join(' ') }}"


# ---------------------------------------------------------------------------
# Alertes
# ---------------------------------------------------------------------------
def push_dag_metric(dag_id: str, outcome: str, url: str | None = None) -> bool:
    """Pousse `waba_dag_last_<outcome>_timestamp_seconds{dag_id}` au Pushgateway.

    Sert à l'alerte « dag_regulatory_report en échec à J+1 06h00 UTC ». Sans URL
    (docker compose), ne fait rien. Ne lève jamais d'exception."""
    url = url if url is not None else PUSHGATEWAY_URL
    if not url:
        return False
    metric = f"waba_dag_last_{outcome}_timestamp_seconds"
    body = f"# TYPE {metric} gauge\n{metric} {time.time():.0f}\n".encode()
    try:
        req = urllib.request.Request(f"{url.rstrip('/')}/metrics/job/airflow/dag_id/{dag_id}",
                                     data=body, method="POST",
                                     headers={"Content-Type": "text/plain"})
        urllib.request.urlopen(req, timeout=5)  # noqa: S310 (URL de l'infrastructure)
        return True
    except Exception as exc:
        log.warning("pushgateway injoignable : %s", exc)
        return False


def notify_success(context) -> None:
    """Callback de succès de la dernière tâche d'un DAG (horodatage pour Grafana)."""
    push_dag_metric(context["ti"].dag_id, "success")


def notify_failure(context) -> None:
    """Callback d'échec : log structuré + POST JSON sur un webhook si configuré."""
    ti = context["ti"]
    push_dag_metric(ti.dag_id, "failure")
    payload = {
        "text": (f":red_circle: WABA — échec {ti.dag_id}.{ti.task_id} "
                 f"(run {context['run_id']}, tentative {ti.try_number})"),
        "dag_id": ti.dag_id, "task_id": ti.task_id, "run_id": context["run_id"],
        "try_number": ti.try_number, "exception": str(context.get("exception")),
    }
    log.error("ALERT %s", json.dumps(payload))
    url = Variable.get("alert_webhook_url", default="")
    if url:
        try:
            req = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10)  # noqa: S310 (URL maîtrisée par l'admin)
        except Exception as exc:  # une alerte ne doit jamais masquer l'erreur d'origine
            log.warning("webhook d'alerte injoignable : %s", exc)


def notify_retry(context) -> None:
    ti = context["ti"]
    log.warning("RETRY %s", json.dumps({"dag_id": ti.dag_id, "task_id": ti.task_id,
                                        "try_number": ti.try_number,
                                        "exception": str(context.get("exception"))}))


DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": 2.0,     # 1 min, 2 min, ... (jobs idempotents : rejouables)
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": notify_failure,
    "on_retry_callback": notify_retry,
}

COUNTRY_PARAMS = {
    "countries": Param(
        None, type=["null", "array"], items={"type": "string", "enum": COUNTRIES},
        title="Pays à traiter",
        description="Vide = Variable `waba_countries` (défaut : les 8 pays)."),
}


@task
def resolve_countries(params=None, dag_run=None) -> list[str]:
    """Liste des pays du run : conf du déclenchement > Variable > tous les pays."""
    requested = (params or {}).get("countries")
    if not requested:
        requested = Variable.get("waba_countries", default=COUNTRIES, deserialize_json=True)
    countries = [c.upper() for c in requested]
    unknown = sorted(set(countries) - set(COUNTRIES))
    if unknown:
        raise ValueError(f"Pays inconnus : {unknown}")
    log.info("pays traités : %s", countries)
    return countries


# ---------------------------------------------------------------------------
# Opérateur Spark
# ---------------------------------------------------------------------------
class SparkDockerJobOperator(DockerOperator):
    """Exécute `spark-submit /opt/waba/jobs/<job> <args>` dans un conteneur éphémère.

    Même modèle que KubernetesPodOperator (Level 4) : un conteneur par tâche,
    image versionnée, secrets injectés à l'exécution. `spark.driver.host` reçoit
    l'IP du conteneur pour que les executors puissent joindre le driver."""

    template_fields = (*DockerOperator.template_fields, "job_args")

    def __init__(self, *, job: str, job_args: str = "", **kwargs) -> None:
        self.job = job
        self.job_args = job_args
        kwargs.setdefault("pool", "spark")
        super().__init__(
            image=SPARK_IMAGE,
            command=None,  # construit dans execute(), après rendu des templates
            docker_url=DOCKER_URL,
            network_mode=SPARK_NETWORK,
            auto_remove="success",
            mount_tmp_dir=False,
            tty=False,
            **kwargs,
        )

    def execute(self, context):
        conn = BaseHook.get_connection(S3_CONN_ID)
        extra = conn.extra_dejson
        self.command = [
            "bash", "-c",
            f"spark-submit --conf spark.driver.host=$(hostname -i) "
            f"/opt/waba/jobs/{self.job} {self.job_args}",
        ]
        self._private_environment = {
            "S3_ENDPOINT": extra.get("endpoint_url", "http://minio:9000"),
            "S3_ACCESS_KEY": conn.login,
            "S3_SECRET_KEY": conn.password,
            "AWS_REGION": extra.get("region_name", "us-east-1"),
            "ICEBERG_REST_URI": Variable.get("iceberg_rest_uri",
                                             default="http://iceberg-rest:8181"),
            "ICEBERG_WAREHOUSE": Variable.get("iceberg_warehouse", default="s3://lakehouse/"),
            "PII_HASH_SECRET": Variable.get("pii_hash_secret"),
            "LANDING_BUCKET": Variable.get("landing_bucket", default="raw-landing"),
            "ARCHIVE_BUCKET": Variable.get("archive_bucket", default="archive"),
            "REPORTS_BUCKET": Variable.get("reports_bucket", default="regulatory-reports"),
        }
        log.info("spark job %s %s", self.job, self.job_args)
        return super().execute(context)


# ---------------------------------------------------------------------------
# Level 4 : Spark Operator (Kubernetes)
# ---------------------------------------------------------------------------
SPARK_GROUP, SPARK_VERSION_API, SPARK_PLURAL = "sparkoperator.k8s.io", "v1beta2", "sparkapplications"
TERMINAL_OK = {"COMPLETED"}
TERMINAL_KO = {"FAILED", "SUBMISSION_FAILED", "FAILING", "INVALIDATING"}


def spark_app_name(job: str, dag_id: str, task_id: str, run_id: str, try_number: int) -> str:
    """Nom DNS-1123 court (le pod driver ajoute « -driver », limite 63 caractères)."""
    stem = job.removesuffix(".py").replace("_", "-")[:18]
    digest = hashlib.sha1(f"{dag_id}|{task_id}|{run_id}|{try_number}".encode()).hexdigest()[:10]
    return f"waba-{stem}-{digest}"


def build_spark_application(name: str, job: str, args: list[str], *, image: str = SPARK_IMAGE,
                            namespace: str = SPARK_NAMESPACE,
                            service_account: str = SPARK_SERVICE_ACCOUNT,
                            env_secret: str = SPARK_ENV_SECRET,
                            labels: dict | None = None) -> dict:
    """Ressource SparkApplication (Spark Operator v2) d'un job PySpark batch.

    Aucun secret dans la ressource : le driver reçoit ses variables d'environnement
    depuis le Secret `env_secret` via un pod template (envFrom). Les executors
    n'en ont pas besoin (la configuration S3/Iceberg leur est transmise par Spark)."""
    labels = {"app.kubernetes.io/part-of": "waba", "app": "spark-batch",
              "waba/job": job.removesuffix(".py").replace("_", "-"), **(labels or {})}
    driver_template = {"spec": {"containers": [{
        "name": "spark-kubernetes-driver",
        "envFrom": [{"secretRef": {"name": env_secret}}]}]}}
    return {
        "apiVersion": f"{SPARK_GROUP}/{SPARK_VERSION_API}",
        "kind": "SparkApplication",
        "metadata": {"name": name, "namespace": namespace, "labels": labels},
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": image,
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": f"local:///opt/waba/jobs/{job}",
            "arguments": args,
            "sparkVersion": "3.5.3",
            "restartPolicy": {"type": "Never"},     # les retries sont gérés par Airflow
            "timeToLiveSeconds": 3600,              # nettoyage des applications terminées
            "sparkConf": {"spark.sql.shuffle.partitions": "8",
                          "spark.kubernetes.submission.waitAppCompletion": "false"},
            "driver": {"cores": 1, "memory": os.environ.get("WABA_SPARK_DRIVER_MEMORY", "1g"),
                       "serviceAccount": service_account,
                       "labels": labels, "template": driver_template,
                       # logs JSON du driver collectés par Promtail -> Loki
                       "annotations": {"waba/log-format": "json"}},
            "executor": {"instances": 1, "cores": 2,
                         "memory": os.environ.get("WABA_SPARK_EXECUTOR_MEMORY", "1536m"),
                         "labels": labels},
        },
    }


class SparkK8sJobOperator(BaseOperator):
    """Soumet `jobs/<job>` au Spark Operator et attend la fin (COMPLETED / FAILED).

    Idempotent : le nom de l'application dépend du run et de la tentative ; une
    application restée d'une tentative précédente est supprimée avant soumission."""

    template_fields = ("job_args",)
    ui_color = "#f4a261"

    def __init__(self, *, job: str, job_args: str = "", poll_interval: int = 15,
                 **kwargs) -> None:
        kwargs.setdefault("pool", "spark")
        super().__init__(**kwargs)
        self.job, self.job_args, self.poll_interval = job, job_args, poll_interval
        self._app_name: str | None = None

    def _api(self):
        from kubernetes import client, config
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        return client.CustomObjectsApi(), client.CoreV1Api()

    def execute(self, context):
        from kubernetes.client.rest import ApiException
        ti = context["ti"]
        name = spark_app_name(self.job, ti.dag_id, ti.task_id, context["run_id"],
                              ti.try_number)
        self._app_name = name
        body = build_spark_application(name, self.job, self.job_args.split(),
                                       labels={"waba/dag": ti.dag_id[:63]})
        api, core = self._api()
        coords = (SPARK_GROUP, SPARK_VERSION_API, SPARK_NAMESPACE, SPARK_PLURAL)
        try:
            api.delete_namespaced_custom_object(*coords, name)
        except ApiException as exc:
            if exc.status != 404:
                raise
        api.create_namespaced_custom_object(*coords, body)
        log.info("SparkApplication %s/%s soumise : %s %s", SPARK_NAMESPACE, name, self.job,
                 self.job_args)
        state = None
        while True:
            obj = api.get_namespaced_custom_object(*coords, name)
            status = obj.get("status") or {}
            new_state = (status.get("applicationState") or {}).get("state")
            if new_state != state:
                log.info("état %s : %s", name, new_state)
                state = new_state
            if state in TERMINAL_OK | TERMINAL_KO:
                break
            time.sleep(self.poll_interval)
        pod = (status.get("driverInfo") or {}).get("podName")
        if pod:
            try:
                logs = core.read_namespaced_pod_log(pod, SPARK_NAMESPACE, tail_lines=200)
                for line in logs.splitlines():
                    log.info("[driver] %s", line)
            except ApiException as exc:
                log.warning("logs du driver indisponibles : %s", exc.reason)
        if state in TERMINAL_KO:
            msg = (status.get("applicationState") or {}).get("errorMessage", "")
            raise RuntimeError(f"SparkApplication {name} en échec ({state}) {msg[:500]}")
        return name

    def on_kill(self) -> None:
        if not self._app_name:
            return
        try:
            api, _ = self._api()
            api.delete_namespaced_custom_object(SPARK_GROUP, SPARK_VERSION_API,
                                                SPARK_NAMESPACE, SPARK_PLURAL, self._app_name)
        except Exception as exc:
            log.warning("suppression de %s impossible : %s", self._app_name, exc)


# Les DAGs utilisent `SparkJobOperator` : la cible dépend du déploiement.
SparkJobOperator = SparkK8sJobOperator if SPARK_BACKEND == "k8s" else SparkDockerJobOperator
