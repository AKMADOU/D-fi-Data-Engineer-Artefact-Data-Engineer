"""Import idempotent des dashboards WABA et création des rôles Superset (Level 4).

Exécuté par le Job Kubernetes superset-init, après `superset db upgrade` et
`superset init`. Variables d'environnement :
  TRINO_HOST, TRINO_PORT        Trino (HTTPS interne)
  TRINO_SUPERSET_PASSWORD       mot de passe du compte de service « superset » dans Trino
  SUPERSET_ADMIN_USERNAME       propriétaire des objets importés (compte local)
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import waba_bundle as B  # noqa: E402

# Rôle Superset -> rôle de base dont il hérite des droits d'interface
ROLE_BASE = "Gamma"


def trino_uri() -> str:
    host = os.environ.get("TRINO_HOST", "trino.serving.svc.cluster.local")
    port = os.environ.get("TRINO_PORT", "8443")
    # Le mot de passe n'est jamais dans l'URI exportable : il est passé à l'import.
    return f"trino://superset:XXXXXXXXXX@{host}:{port}/iceberg"


def run(app=None) -> dict:
    from flask import g

    if app is None:
        from superset.app import create_app
        app = create_app()
    with app.app_context():
        from superset import db, security_manager as sm
        from superset.commands.dashboard.importers.dispatcher import ImportDashboardsCommand
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.dashboard import Dashboard

        admin = sm.find_user(username=os.environ.get("SUPERSET_ADMIN_USERNAME", "admin"))
        if admin is None:
            raise SystemExit("compte admin Superset introuvable (superset fab create-admin)")
        g.user = admin
        contents = B.bundle(trino_uri())
        ImportDashboardsCommand(
            contents, passwords={B.DB_FILE: os.environ["TRINO_SUPERSET_PASSWORD"]},
            overwrite=True).run()

        # Rôles métier : droits d'interface de Gamma + accès aux seuls datasets autorisés
        base = sm.find_role(ROLE_BASE)
        report = {}
        for role_name, datasets in B.ROLE_DATASETS.items():
            role = sm.find_role(role_name) or sm.add_role(role_name)
            perms = set(base.permissions) if base else set()
            for name in datasets:
                table = db.session.query(SqlaTable).filter_by(uuid=B.uid("dataset", name)).one()
                pv = sm.add_permission_view_menu("datasource_access", table.perm)
                perms.add(pv)
            role.permissions = list(perms)
            report[role_name] = len(datasets)
        db.session.commit()
        dashboards = db.session.query(Dashboard).filter(
            Dashboard.slug.in_(list(B.DASHBOARDS))).all()
        result = {"dashboards": sorted(d.slug for d in dashboards),
                  "charts": sum(len(d.slices) for d in dashboards), "roles": report}
        print(result)
        return result


if __name__ == "__main__":
    run()
