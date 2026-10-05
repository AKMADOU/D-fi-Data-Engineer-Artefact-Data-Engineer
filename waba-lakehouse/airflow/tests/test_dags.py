"""Tests des DAGs : import sans erreur, dépendances, planification, commandes Spark.

    docker compose exec airflow-scheduler python -m pytest -q /opt/airflow/tests
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest import mock

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS_DIR))

# Connection / Variables simulées (mêmes noms qu'en production, valeurs factices)
FAKE_ENV = {
    "AIRFLOW_CONN_MINIO_S3": json.dumps({
        "conn_type": "aws", "login": "test-key", "password": "test-secret",
        "extra": {"endpoint_url": "http://minio:9000", "region_name": "us-east-1"}}),
    "AIRFLOW_VAR_PII_HASH_SECRET": "test-pii-secret",
}


@pytest.fixture(scope="module")
def dagbag():
    from airflow.dag_processing.dagbag import DagBag
    return DagBag(dag_folder=str(DAGS_DIR))


EXPECTED = {"dag_ingest_raw", "dag_bronze_to_silver", "dag_silver_to_gold",
            "dag_regulatory_report"}


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}
    assert EXPECTED <= set(dagbag.dag_ids)


def test_default_args_retries_and_alerts(dagbag):
    for dag_id in EXPECTED:
        for t in dagbag.get_dag(dag_id).tasks:
            assert t.retries >= 2, t.task_id
            assert t.on_failure_callback, t.task_id


def test_ingest_dependencies(dagbag):
    dag = dagbag.get_dag("dag_ingest_raw")
    t = dag.task_dict
    assert t["detect_new_files"].upstream_task_ids == {"resolve_countries"}
    assert t["ingest_referentials"].upstream_task_ids == {"detect_new_files"}
    for ds in ("bank_transactions", "insurance_operations", "mobile_money_payments",
               "loan_repayments"):
        assert t[f"ingest_{ds}"].upstream_task_ids == {"ingest_referentials"}
    assert len(t["publish_bronze"].upstream_task_ids) == 4
    assert [o.uri for o in t["publish_bronze"].outlets] == ["s3://lakehouse/bronze"]


def test_layers_chained_by_assets(dagbag):
    """silver est déclenché par l'asset bronze, gold par l'asset silver."""
    silver = dagbag.get_dag("dag_bronze_to_silver")
    gold = dagbag.get_dag("dag_silver_to_gold")
    assert "s3://lakehouse/bronze" in str(silver.timetable.asset_condition)
    assert "s3://lakehouse/silver" in str(gold.timetable.asset_condition)
    assert gold.task_dict["publish_gold"].outlets[0].uri == "s3://lakehouse/gold"


def test_regulatory_schedule_j_plus_1(dagbag):
    dag = dagbag.get_dag("dag_regulatory_report")
    # CronTriggerTimetable (défaut Airflow 3) : ds = date d'exécution -> D = ds - 1 jour
    assert dag.timetable.expression == "30 0 * * *" and str(dag.timetable.timezone) == "UTC"
    assert dag.task_dict["check_exports"].upstream_task_ids >= {"build_regulatory_report"}


def test_report_date_template():
    from datetime import date, timedelta
    from types import SimpleNamespace

    import jinja2
    from dag_regulatory_report import REPORT_DATE
    macros = SimpleNamespace(ds_add=lambda ds, n: str(date.fromisoformat(ds) + timedelta(days=n)))
    render = lambda p: jinja2.Template(REPORT_DATE).render(params=p, ds="2026-09-26",  # noqa
                                                            macros=macros)
    assert render({"report_date": None}) == "2026-09-25"          # J+1 : veille
    assert render({"report_date": "2026-06-15"}) == "2026-06-15"  # rejeu d'une date


def test_spark_job_command_and_private_env(dagbag):
    """Le job reçoit les identifiants en variables privées, jamais dans la commande."""
    from airflow.providers.docker.operators.docker import DockerOperator
    op = dagbag.get_dag("dag_silver_to_gold").task_dict["gold_insurance"]
    op.job_args = "--domain insurance --countries CI SN"   # args déjà rendus
    with mock.patch.dict(os.environ, FAKE_ENV), \
            mock.patch.object(DockerOperator, "execute", return_value=None) as parent:
        op.execute(context={})
    parent.assert_called_once()
    cmd = " ".join(op.command)
    assert "spark-submit" in cmd and "/opt/waba/jobs/gold.py --domain insurance" in cmd
    assert "test-secret" not in cmd
    env = op._private_environment
    assert env["S3_ACCESS_KEY"] == "test-key" and env["S3_SECRET_KEY"] == "test-secret"
    assert env["PII_HASH_SECRET"] == "test-pii-secret"
    assert op.pool == "spark" and op.auto_remove == "success" and op.mount_tmp_dir is False


def test_failure_callback_posts_webhook():
    from waba import common
    ti = mock.Mock(dag_id="d", task_id="t", try_number=3)
    with mock.patch.object(common.Variable, "get", return_value="http://hook.local"), \
            mock.patch("urllib.request.urlopen") as post:
        common.notify_failure({"ti": ti, "run_id": "r1", "exception": RuntimeError("boom")})
    body = json.loads(post.call_args[0][0].data)
    assert body["dag_id"] == "d" and "boom" in body["exception"]


# ------------------------------------------------------------------ Level 4 : Kubernetes
def test_spark_application_has_no_secret_and_valid_name():
    from waba import common
    name = common.spark_app_name("regulatory_report.py", "dag_regulatory_report",
                                 "build_regulatory_report", "scheduled__2026-09-30", 3)
    assert len(name + "-driver") <= 63 and name == name.lower()
    app = common.build_spark_application(name, "silver.py", ["--countries", "CI", "SN"],
                                         env_secret="waba-spark-env")
    spec = app["spec"]
    assert app["kind"] == "SparkApplication" and spec["mainApplicationFile"].endswith("silver.py")
    assert spec["arguments"] == ["--countries", "CI", "SN"]
    env_from = spec["driver"]["template"]["spec"]["containers"][0]["envFrom"]
    assert env_from == [{"secretRef": {"name": "waba-spark-env"}}]
    assert "secret" not in json.dumps(spec["sparkConf"]).lower()
    assert spec["restartPolicy"] == {"type": "Never"}


def test_k8s_operator_submits_and_waits():
    from waba import common
    op = common.SparkK8sJobOperator(task_id="gold_banking", job="gold.py",
                                    job_args="--domain banking --countries CI", poll_interval=0)
    api, core = mock.Mock(), mock.Mock()
    states = iter(["SUBMITTED", "RUNNING", "COMPLETED"])
    api.get_namespaced_custom_object.side_effect = lambda *a: {
        "status": {"applicationState": {"state": next(states)},
                   "driverInfo": {"podName": "p-driver"}}}
    core.read_namespaced_pod_log.return_value = '{"status":"SUCCESS"}'
    ti = mock.Mock(dag_id="dag_silver_to_gold", task_id="gold_banking", try_number=1)
    with mock.patch.object(op, "_api", return_value=(api, core)):
        name = op.execute({"ti": ti, "run_id": "manual__1"})
    body = api.create_namespaced_custom_object.call_args[0][4]
    assert body["metadata"]["name"] == name and body["spec"]["arguments"][:2] == ["--domain",
                                                                                "banking"]
    assert api.get_namespaced_custom_object.call_count == 3


def test_k8s_operator_raises_on_failed_app():
    from waba import common
    op = common.SparkK8sJobOperator(task_id="t", job="silver.py", poll_interval=0)
    api, core = mock.Mock(), mock.Mock()
    api.get_namespaced_custom_object.return_value = {
        "status": {"applicationState": {"state": "FAILED", "errorMessage": "OOMKilled"}}}
    ti = mock.Mock(dag_id="d", task_id="t", try_number=1)
    with mock.patch.object(op, "_api", return_value=(api, core)), \
            pytest.raises(RuntimeError, match="OOMKilled"):
        op.execute({"ti": ti, "run_id": "r"})


def test_pushgateway_metric():
    from waba import common
    with mock.patch("urllib.request.urlopen") as post:
        assert common.push_dag_metric("dag_regulatory_report", "success", url="http://pg:9091")
    req = post.call_args[0][0]
    assert req.full_url == "http://pg:9091/metrics/job/airflow/dag_id/dag_regulatory_report"
    assert b"waba_dag_last_success_timestamp_seconds " in req.data
    assert common.push_dag_metric("d", "failure", url="") is False


def test_regulatory_last_task_pushes_success(dagbag):
    from waba.common import notify_success
    t = dagbag.get_dag("dag_regulatory_report").task_dict["check_exports"]
    cb = t.on_success_callback
    assert cb == notify_success or notify_success in (cb or [])
