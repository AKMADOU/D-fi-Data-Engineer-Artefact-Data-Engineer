"""Tests du bundle Superset (sans Superset) : cohérence datasets / graphiques / dashboards.

L'import réel a été validé sur un Superset 6.1 local (voir docs/ARCHITECTURE_L4.md)."""
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bootstrap"))
import waba_bundle as B  # noqa: E402


def test_bundle_consistency():
    files = B.bundle("trino://superset:XXXXXXXXXX@trino:8443/iceberg")
    docs = {p: yaml.safe_load(c) for p, c in files.items()}
    datasets = {d["uuid"]: d for p, d in docs.items() if p.startswith("datasets/")}
    charts = {d["uuid"]: d for p, d in docs.items() if p.startswith("charts/")}
    dashboards = [d for p, d in docs.items() if p.startswith("dashboards/")]
    assert len(dashboards) == 3 and len(charts) == len(B.CHARTS)
    for c in charts.values():
        ds = datasets[c["dataset_uuid"]]
        names = {m["metric_name"] for m in ds["metrics"]}
        cols = {x["column_name"] for x in ds["columns"]}
        p = c["params"]
        for m in p.get("metrics", []) + [p[k] for k in ("metric",) if k in p]:
            assert m in names, (c["slice_name"], m)
        for k in ("x_axis", "entity"):
            if k in p:
                assert p[k] in cols, (c["slice_name"], p[k])
    for d in dashboards:
        # le filtre « Pays » : chaque graphique du dashboard a une colonne country_code
        in_dash = [v["meta"]["uuid"] for v in d["position"].values()
                   if isinstance(v, dict) and v.get("type") == "CHART"]
        for u in in_dash:
            cols = {x["column_name"] for x in datasets[charts[u]["dataset_uuid"]]["columns"]}
            assert "country_code" in cols
    assert "XXXXXXXXXX" in docs[B.DB_FILE]["sqlalchemy_uri"]    # jamais de mot de passe
    assert docs[B.DB_FILE]["impersonate_user"] is True
