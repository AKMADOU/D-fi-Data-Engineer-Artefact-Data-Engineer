# WABA Group — Data Lakehouse multi-pays

Plateforme data de bout en bout pour **WestAfrica BancAssur Group** (banque, assurance,
mobile money) dans **8 pays d'Afrique de l'Ouest** : CI, SN, ML, BF, GN, TG, BJ, GH.
Elle couvre l'ingestion, l'architecture médaillon, le reporting BCEAO / CIMA, la détection
de fraude en temps réel et un déploiement Kubernetes gouverné et observable.

## Les 4 levels

| Level | Contenu | Stack |
|---|---|---|
| **1. Ingestion** | Génération de données multi-pays, validation, tables Iceberg | Streamlit · MinIO · Spark · Iceberg · Trino |
| **2. Médaillon** | Bronze → Silver → Gold, 7 KPIs, reporting réglementaire J+1 | Airflow 3 |
| **3. Lambda** | Fraude, AML et liquidité en quasi-temps réel ; requête batch + streaming | NiFi · Kafka · Spark Structured Streaming |
| **4. Production** | Kubernetes, dashboards, SSO, catalogue, observabilité | Kustomize · Superset · Keycloak · OpenMetadata · Prometheus · Loki · Grafana |

## Architecture

![Architecture du lakehouse WABA](Architecture%20moderne%20d’une%20plateforme%20de%20données.png)

## Démarrage rapide (Levels 1 à 3)

Prérequis : Docker Desktop (12 Go de RAM minimum, 16 Go conseillés), `make`, `python3`.

```bash
cp .env.example .env              # valeurs d'exemple utilisables en local
make up                           # construit et démarre la stack
make init                         # crée les tables Iceberg
make generate-referentials CUSTOMERS=50000 ACCOUNTS=80000 && make generate-events
make airflow-pipeline             # Bronze -> Silver -> Gold via Airflow
make kpis                         # affiche les 7 KPIs
make streaming-up && make fraud-demo && make streaming-check   # temps réel
make lambda                       # requête Lambda : Iceberg + Kafka
```

👉 Pas à pas détaillé, avec un contrôle à chaque étape : **[GUIDE_DEMARRAGE.md](GUIDE_DEMARRAGE.md)**

**Level 4 (Kubernetes)** : `make k8s-up` (Minikube, 24 Go de RAM). Voir
[README_COMPLET.md](README_COMPLET.md#4-ter-level-4--kubernetes-gouvernance-observabilité).

## Interfaces (docker compose)

| Service | URL |
|---|---|
| Streamlit (générateur) | http://localhost:8501 |
| Airflow | http://localhost:8090 |
| MinIO | http://localhost:9001 |
| Trino (SQL, DBeaver) | `localhost:8088` |
| NiFi | https://localhost:8443/nifi |
| Kafka UI | http://localhost:8084 |

Les identifiants sont dans `.env`.

## Points clés

- **Idempotence partout** : `MERGE` sur Iceberg, réécriture par partition (pays × mois), alertes à identifiant déterministe.
- **Qualité** : schéma explicite, motifs de rejet tracés, quarantaine des clés orphelines, file de rejets Kafka (DLQ).
- **Données personnelles** : identifiants pseudonymisés (SHA-256 salé) dès Silver ; Trino filtre les lignes par pays et masque des colonnes selon le rôle.
- **Aucun secret dans git** : `.env`, Secrets Kubernetes générés localement.
- **Tests** : générateur, Spark (dont Structured Streaming), DAGs Airflow, provisioning NiFi et OpenMetadata, alertes (promtool), manifestes (kubeconform). Lancer `make test`.

## Documentation

| Document | Contenu |
|---|---|
| [GUIDE_DEMARRAGE.md](GUIDE_DEMARRAGE.md) | Lancer les Levels 1 à 3 pas à pas |
| [README_COMPLET.md](README_COMPLET.md) | Toutes les commandes, critères d'évaluation, dépannage |
| [docs/WRITEUP.md](docs/WRITEUP.md) | Write-up technique : choix, compromis, limites |
| [docs/](docs/) | Architecture détaillée de chaque level |
| [README.env.example](README.env.example) | Toutes les variables d'environnement |

## Structure

```
generator/   Streamlit + CLI de génération        airflow/     DAGs et opérateurs
spark/       jobs batch et streaming + tests       nifi/        flux NiFi (API REST)
kafka/       création des topics                   trino/       catalogues Iceberg et Kafka
sql/         requêtes de contrôle et de démo       superset/    dashboards et SSO
k8s/         manifestes Kubernetes (Level 4)       governance/  OpenMetadata
scripts/     utilitaires (Trino, Airflow, K8s)     docs/        documentation
```