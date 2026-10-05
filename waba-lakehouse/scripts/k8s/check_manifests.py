#!/usr/bin/env python3
"""Contrôles de cohérence du rendu Kustomize (complément de kubeconform) :

  * chaque Secret / ConfigMap référencé (env, envFrom, volumes) existe dans le même
    namespace, et chaque clé référencée existe ;
  * chaque Service sélectionne au moins un pod (labels du template) ;
  * chaque backend d'Ingress est un Service existant, sur un port déclaré ;
  * chaque ServiceAccount utilisé existe ;
  * aucune valeur ressemblant à un secret en clair dans les variables d'environnement.

    kustomize build . | python3 scripts/k8s/check_manifests.py
"""
from __future__ import annotations

import re
import sys

import yaml

SECRETISH = re.compile(r"(password|secret|token|key)$", re.I)
ALLOWED_PLAIN = {"KC_BOOTSTRAP_ADMIN_USERNAME", "SUPERSET_OIDC_CLIENT_ID", "GF_SECURITY_ADMIN_USER",
                 "NIFI_SENSITIVE_PROPS_KEY"}


def pod_specs(doc):
    kind = doc["kind"]
    spec = doc.get("spec", {})
    if kind in ("Deployment", "StatefulSet", "DaemonSet", "Job"):
        return [spec["template"]]
    if kind == "CronJob":
        return [spec["jobTemplate"]["spec"]["template"]]
    return []


def main() -> int:
    docs = [d for d in yaml.safe_load_all(sys.stdin) if d]
    objs = {(d["kind"], d["metadata"].get("namespace"), d["metadata"]["name"]): d for d in docs}
    errors = []

    def keys(kind, ns, name):
        d = objs.get((kind, ns, name))
        if d is None:
            return None
        return set((d.get("data") or {}) | (d.get("stringData") or {}))

    def need(kind, ns, name, key=None, where="", optional=False):
        ks = keys(kind, ns, name)
        if ks is None:
            if not optional:
                errors.append(f"{where}: {kind} {ns}/{name} introuvable")
        elif key and key not in ks and not optional:
            errors.append(f"{where}: clé {key} absente de {kind} {ns}/{name} ({sorted(ks)})")

    for d in docs:
        ns = d["metadata"].get("namespace")
        where = f'{d["kind"]} {ns}/{d["metadata"]["name"]}'
        for tpl in pod_specs(d):
            spec = tpl["spec"]
            sa = spec.get("serviceAccountName")
            if sa and ("ServiceAccount", ns, sa) not in objs:
                errors.append(f"{where}: ServiceAccount {sa} introuvable")
            for c in spec.get("containers", []) + spec.get("initContainers", []):
                for e in c.get("env", []):
                    vf = e.get("valueFrom", {})
                    if "secretKeyRef" in vf:
                        r = vf["secretKeyRef"]
                        need("Secret", ns, r["name"], r["key"], where, r.get("optional", False))
                    if "configMapKeyRef" in vf:
                        r = vf["configMapKeyRef"]
                        need("ConfigMap", ns, r["name"], r["key"], where)
                    if "value" in e and SECRETISH.search(e["name"]) and e["name"] not in ALLOWED_PLAIN:
                        errors.append(f"{where}: {e['name']} en clair")
                for ef in c.get("envFrom", []):
                    if "secretRef" in ef:
                        need("Secret", ns, ef["secretRef"]["name"], where=where)
                    if "configMapRef" in ef:
                        need("ConfigMap", ns, ef["configMapRef"]["name"], where=where)
            for v in spec.get("volumes", []):
                if "configMap" in v:
                    need("ConfigMap", ns, v["configMap"]["name"], where=where)
                if "secret" in v:
                    need("Secret", ns, v["secret"]["secretName"], where=where)
                if "persistentVolumeClaim" in v:
                    if ("PersistentVolumeClaim", ns, v["persistentVolumeClaim"]["claimName"]) not in objs:
                        errors.append(f"{where}: PVC {v['persistentVolumeClaim']['claimName']} introuvable")
    labels_by_ns = {}
    for d in docs:
        for tpl in pod_specs(d):
            labels_by_ns.setdefault(d["metadata"].get("namespace"), []).append(
                tpl.get("metadata", {}).get("labels", {}))
    for d in docs:
        ns = d["metadata"].get("namespace")
        if d["kind"] == "Service":
            sel = d["spec"].get("selector") or {}
            if not any(all(l.get(k) == v for k, v in sel.items()) for l in labels_by_ns.get(ns, [])):
                errors.append(f"Service {ns}/{d['metadata']['name']} : aucun pod pour {sel}")
        if d["kind"] == "Ingress":
            for rule in d["spec"]["rules"]:
                for p in rule["http"]["paths"]:
                    svc = p["backend"]["service"]
                    s = objs.get(("Service", ns, svc["name"]))
                    if s is None:
                        errors.append(f"Ingress {ns}: Service {svc['name']} introuvable")
                    elif svc["port"]["number"] not in {pt["port"] for pt in s["spec"]["ports"]}:
                        errors.append(f"Ingress {ns}: port {svc['port']['number']} absent de {svc['name']}")
            for tls in d["spec"].get("tls", []):
                need("Secret", ns, tls["secretName"], where=f"Ingress {ns}")
    for e in errors:
        print("ERREUR", e)
    print(f"{len(docs)} ressources contrôlées, {len(errors)} erreur(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
