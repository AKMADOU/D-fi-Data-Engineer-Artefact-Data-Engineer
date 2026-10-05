# Raccourcis WABA Lakehouse — Levels 1 à 4.  `make help` pour la liste.
.DEFAULT_GOAL := help
SHELL := /bin/bash
COMPOSE := docker compose
GEN := $(COMPOSE) exec -T generator python -m waba_gen.cli
AIRFLOW := $(COMPOSE) exec -T airflow-apiserver airflow

# Taille des référentiels (machine modeste : make demo CUSTOMERS=50000 ACCOUNTS=80000)
CUSTOMERS ?= 500000
ACCOUNTS  ?= 800000
# Pays pour les commandes manuelles (ex. make silver COUNTRIES="CI SN")
COUNTRIES ?= CI SN ML BF GN TG BJ GH
# Date de reporting réglementaire (données générées = trimestre précédent)
# Par défaut : le 15e jour avant le début du trimestre en cours, donc toujours DANS le
# trimestre précédent que le générateur produit (ex. le 2026-10-04 -> 2026-09-16).
D ?= $(shell python3 -c "import datetime as d;t=d.date.today();q=d.date(t.year,3*((t.month-1)//3)+1,1);print(q-d.timedelta(days=15))")

.PHONY: help env env-upgrade up down clean ps logs build-spark init init-raw \
        generate-referentials generate-events ingest ingest-raw bootstrap-bronze \
        idempotency trino trino-sql analytics quality medallion kpis regulatory-sql \
        silver gold regulatory airflow-pipeline airflow-regulatory airflow-status \
        test demo demo-l2 streaming-up nifi-provision topics stream-logs fraud-demo \
        dlq-demo lambda streaming-check dlq test-nifi demo-l3 \
        k8s-up k8s-images k8s-secrets k8s-identity k8s-apply k8s-validate k8s-status k8s-hosts \
        k8s-credentials k8s-data k8s-pipeline k8s-regulatory k8s-fraud k8s-governance k8s-superset \
        k8s-alert-demo k8s-down k8s-destroy test-l4
KCTL := kubectl
K8S_AF := AIRFLOW_EXEC="kubectl -n processing exec deploy/airflow-scheduler -- airflow"
K8S_GEN := $(KCTL) -n ingestion exec deploy/generator -- python -m waba_gen.cli

help: ## Affiche cette aide
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-22s\033[0m %s\n",$$1,$$2}'

# ----------------------------------------------------------------- environnement
env: ## Crée .env depuis .env.example (si absent)
	@test -f .env || (cp .env.example .env && echo ".env créé — pensez à changer les mots de passe")

env-upgrade: ## Ajoute à .env les nouvelles variables (migration Level 1 -> Level 2)
	./scripts/env_upgrade.sh

up: env ## Construit et démarre toute la stack (L1 + Airflow)
	$(COMPOSE) up -d --build
	@echo "Streamlit :8501 | MinIO :9001 | Spark :8080 | Trino :8088 | Airflow :8090"
	@echo "L3 : NiFi https://localhost:8443/nifi | Kafka UI :8084 (si COMPOSE_PROFILES=streaming)"

down: ## Arrête la stack (données conservées)
	$(COMPOSE) down

clean: ## Arrête la stack ET supprime les volumes (données perdues)
	$(COMPOSE) down -v

ps: ## État des services
	$(COMPOSE) ps

logs: ## Logs (ex. make logs S=airflow-scheduler)
	$(COMPOSE) logs -f $(S)

build-spark: ## Reconstruit l'image Spark (après modification des jobs, pour Airflow)
	$(COMPOSE) build spark-master

# -------------------------------------------------------------------- Level 1
init: ## Crée les namespaces (bronze, silver, gold, audit) et les tables Bronze
	./scripts/spark-submit.sh create_tables.py --namespace bronze

init-raw: ## [L1] Crée les tables raw.* du Level 1
	./scripts/spark-submit.sh create_tables.py --namespace raw

generate-referentials: ## Génère les référentiels (CLI, sans Streamlit)
	$(GEN) referentials --customers $(CUSTOMERS) --accounts $(ACCOUNTS)

generate-events: ## Génère les 4 flux sur le dernier trimestre (volumes de l'énoncé)
	$(GEN) events --dataset all --anomaly-rate $(or $(ANOMALY),0.01)

ingest: ## Ingestion manuelle raw-landing -> bronze.* (Airflow le fait tout seul)
	./scripts/spark-submit.sh ingest.py --dataset all --namespace bronze

ingest-raw: ## [L1] Ingestion raw-landing -> raw.*
	./scripts/spark-submit.sh ingest.py --dataset all --namespace raw

bootstrap-bronze: ## Migration L1 -> L2 : charge en Bronze les fichiers déjà archivés
	./scripts/spark-submit.sh ingest.py --dataset all --namespace bronze --source archive

idempotency: ## Rejoue les fichiers archivés et vérifie l'absence de doublons en Bronze
	./scripts/idempotency_check.sh

trino: ## Ouvre le client Trino interactif
	./scripts/trino.sh

trino-sql: ## Exécute un fichier SQL de sql/ (make trino-sql FILE=05_gold_kpis.sql)
	./scripts/trino.sh -f $(FILE)

analytics: ## Requêtes analytiques Bronze (soldes, volumes, entités)
	./scripts/trino.sh -f 02_analytics.sql

quality: ## Contrôles qualité Bronze (orphelins, doublons, devises, rejets)
	./scripts/trino.sh -f 03_quality_checks.sql

# -------------------------------------------------------------------- Level 2
silver: ## Bronze -> Silver manuel (make silver COUNTRIES="CI SN")
	./scripts/spark-submit.sh silver.py --countries $(COUNTRIES)

gold: ## Silver -> Gold manuel, 7 tables de KPIs
	./scripts/spark-submit.sh gold.py --countries $(COUNTRIES)

regulatory: ## Reporting BCEAO/CIMA manuel pour la date D (défaut : dans le trimestre généré)
	./scripts/spark-submit.sh regulatory_report.py --report-date $(D) --countries $(COUNTRIES)

medallion: ## Vérifie les 3 zones (volumes, partitions, unicité, EUR, pseudonymisation)
	./scripts/trino.sh -f 04_medallion.sql

kpis: ## Affiche les 7 KPIs Gold
	./scripts/trino.sh -f 05_gold_kpis.sql

regulatory-sql: ## Affiche les déclarations BCEAO / CIMA
	./scripts/trino.sh -f 06_regulatory.sql

airflow-pipeline: ## Airflow : ingest -> silver -> gold (déclenche et attend les 3 DAGs)
	$(AIRFLOW) dags unpause dag_bronze_to_silver >/dev/null
	$(AIRFLOW) dags unpause dag_silver_to_gold >/dev/null
	export SINCE=$$(date -u +%Y-%m-%dT%H:%M:%S); \
	  ./scripts/airflow_run.sh dag_ingest_raw && \
	  ./scripts/airflow_run.sh --wait-only dag_bronze_to_silver && \
	  ./scripts/airflow_run.sh --wait-only dag_silver_to_gold

airflow-regulatory: ## Airflow : reporting J+1 pour la date D (défaut : dans le trimestre généré)
	./scripts/airflow_run.sh dag_regulatory_report '{"report_date": "$(D)"}'

airflow-status: ## Derniers runs des 4 DAGs
	@for d in dag_ingest_raw dag_bronze_to_silver dag_silver_to_gold dag_regulatory_report; do \
	  echo "== $$d"; $(AIRFLOW) dags list-runs $$d 2>/dev/null | head -5; done

# -------------------------------------------------------------------- tests / démo
# -------------------------------------------------------------------- Level 3
streaming-up: ## [L3] Démarre Kafka, NiFi, Kafka UI et les 2 jobs Structured Streaming
	$(COMPOSE) --profile streaming up -d --build kafka kafka-init kafka-ui nifi nifi-init stream-silver stream-gold
	@echo "NiFi https://localhost:8443/nifi (1-3 min au 1er démarrage) | Kafka UI http://localhost:8084"

nifi-provision: ## [L3] (Re)crée le flux NiFi raw-landing -> Kafka (NIFI_RECREATE=true pour forcer)
	$(COMPOSE) --profile streaming run --rm -e NIFI_RECREATE=$(or $(NIFI_RECREATE),false) nifi-init

topics: ## [L3] Liste les topics et le nombre de messages par topic
	$(COMPOSE) exec -T kafka bash -c '/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 | sort'

stream-logs: ## [L3] Logs JSON des jobs streaming (micro-lots, lag, alertes)
	$(COMPOSE) logs -f --tail 50 stream-silver stream-gold

fraud-demo: ## [L3] Injecte les 5 scénarios (rafale, AML, bank run, pays inhabituel, gros sinistre)
	$(GEN) scenarios --countries $(or $(SCENARIO_COUNTRIES),CI GH)
	@echo "Alertes attendues sous ~30 s : make streaming-check (ou Kafka UI, topics gold-*)"

dlq-demo: ## [L3] Dépose un fichier défectueux : ses lignes doivent arriver dans dlq-financial-events
	$(COMPOSE) exec -T generator python - < scripts/dlq_demo.py

dlq: ## [L3] Affiche les 20 derniers messages de la DLQ
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
	  --topic dlq-financial-events --from-beginning --timeout-ms 5000 2>/dev/null | tail -20

lambda: ## [L3] Requête Lambda Trino : Gold (Iceberg) + silver-bank-transactions (Kafka)
	./scripts/trino.sh -f 07_lambda.sql

streaming-check: ## [L3] Lag NiFi->Kafka, fraîcheur Silver temps réel, alertes, DLQ
	./scripts/trino.sh -f 08_streaming.sql

test-nifi: ## [L3] Tests du provisioning NiFi (faux serveur REST, sans NiFi)
	docker run --rm -v "$$PWD/nifi:/nifi:ro" -w /nifi python:3.12-alpine \
	  sh -c "pip install -q pytest==8.3.3 && python -m pytest -q -p no:cacheprovider tests"

# -------------------------------------------------------------------- Level 4 (Kubernetes)
k8s-up: ## [L4] minikube + images + secrets + TOUTE la plateforme (kubectl apply -k .)
	./scripts/k8s/up.sh

k8s-images: ## [L4] Construit les 5 images du projet dans minikube
	./scripts/k8s/build-images.sh

k8s-secrets: ## [L4] Génère les Secrets hors git (k8s/secrets.env, k8s/.generated/) ; PROFILE=full|light
	python3 scripts/k8s/make_secrets.py $(if $(PROFILE),--profile $(PROFILE),)

k8s-identity: ## [L4] Régénère realm Keycloak + groupes/règles Trino depuis k8s/users.json
	python3 scripts/k8s/render_identity.py

k8s-apply: k8s-secrets ## [L4] Applique les manifestes (une seule commande kubectl)
	$(KCTL) apply --server-side --force-conflicts -k .

k8s-validate: k8s-secrets ## [L4] Rendu Kustomize + schémas (kubeconform si présent) + références croisées
	$(KCTL) kustomize . > /tmp/waba-rendered.yaml
	@command -v kubeconform >/dev/null && kubeconform -strict -summary -ignore-missing-schemas /tmp/waba-rendered.yaml || echo "(kubeconform absent : validation de schéma ignorée)"
	python3 scripts/k8s/check_manifests.py < /tmp/waba-rendered.yaml

k8s-status: ## [L4] État des pods, Jobs et Ingress
	./scripts/k8s/status.sh

k8s-hosts: ## [L4] Ligne /etc/hosts pour les URLs *.waba.local
	./scripts/k8s/hosts.sh

k8s-credentials: ## [L4] Identifiants de démonstration (SSO, consoles)
	./scripts/k8s/credentials.sh

k8s-data: ## [L4] Génère référentiels + événements dans le cluster (CUSTOMERS/ACCOUNTS réglables)
	$(K8S_GEN) referentials --customers $(or $(K8S_CUSTOMERS),50000) --accounts $(or $(K8S_ACCOUNTS),80000)
	$(K8S_GEN) events --dataset all --anomaly-rate 0.01

k8s-pipeline: ## [L4] Airflow sur K8s : ingest -> silver -> gold (jobs via Spark Operator)
	export SINCE=$$(date -u +%Y-%m-%dT%H:%M:%S); \
	  $(K8S_AF) ./scripts/airflow_run.sh dag_ingest_raw && \
	  $(K8S_AF) ./scripts/airflow_run.sh --wait-only dag_bronze_to_silver && \
	  $(K8S_AF) ./scripts/airflow_run.sh --wait-only dag_silver_to_gold

k8s-regulatory: ## [L4] Reporting J+1 pour la date D (défaut : dans le trimestre généré)
	$(K8S_AF) ./scripts/airflow_run.sh dag_regulatory_report '{"report_date": "$(D)"}'

k8s-fraud: ## [L4] Injecte les scénarios de fraude / AML / liquidité (Level 3) dans le cluster
	$(K8S_GEN) scenarios --countries CI GH

k8s-governance: ## [L4] Lance maintenant l'ingestion + documentation OpenMetadata
	$(KCTL) -n governance delete job openmetadata-governance-manual --ignore-not-found
	$(KCTL) -n governance create job openmetadata-governance-manual --from=cronjob/openmetadata-governance
	$(KCTL) -n governance wait --for=condition=complete job/openmetadata-governance-manual --timeout=20m
	$(KCTL) -n governance logs job/openmetadata-governance-manual -c govern | tail -5

k8s-superset: ## [L4] Relance l'import des dashboards Superset
	$(KCTL) -n serving delete job superset-init --ignore-not-found
	$(KCTL) apply --server-side --force-conflicts -k . >/dev/null
	$(KCTL) -n serving wait --for=condition=complete job/superset-init --timeout=20m

k8s-alert-demo: ## [L4] Déclenche une alerte Grafana (A=regulatory|lag|fraud-down|restore)
	./scripts/k8s/alert-demo.sh $(A)

k8s-down: ## [L4] Supprime la plateforme (les PVC des StatefulSets sont conservés)
	$(KCTL) delete -k . --ignore-not-found

k8s-destroy: ## [L4] Supprime le cluster minikube « waba » (toutes ses données)
	minikube -p $(or $(MINIKUBE_PROFILE),waba) delete

test-l4: ## [L4] Tests hors cluster : Superset, NiFi, OpenMetadata, alertes, identité, manifestes
	python3 -m pytest -q -p no:cacheprovider superset/tests nifi/tests governance/tests
	python3 scripts/k8s/render_identity.py --check
	@command -v promtool >/dev/null && promtool test rules k8s/monitoring/tests/alert-tests.yml || echo "(promtool absent : tests d'alertes ignorés)"
	$(MAKE) k8s-validate

test: ## Tests unitaires : générateur, Spark (validation + médaillon), DAGs Airflow
	$(COMPOSE) exec -T generator python -m pytest -q tests
	$(COMPOSE) exec -T -w /opt/waba spark-master python3 -m pytest -q -p no:cacheprovider tests
	$(COMPOSE) exec -T -w /opt/airflow airflow-scheduler python -m pytest -q -p no:cacheprovider tests

demo: init generate-referentials generate-events ingest quality ## [L1] Pipeline manuel complet

demo-l2: init generate-referentials generate-events airflow-pipeline airflow-regulatory medallion kpis ## [L2] Pipeline complet orchestré par Airflow

demo-l3: streaming-up fraud-demo dlq-demo ## [L3] Démarre le temps réel, injecte fraudes + DLQ
	@echo "Patientez ~30 s puis : make streaming-check && make lambda"
