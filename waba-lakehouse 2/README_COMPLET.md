# WABA Group — Data Lakehouse multi-pays (Levels 1 à 4)

> 🚀 **Pour tout lancer pas à pas (Levels 1 à 3), suis le [guide de démarrage](GUIDE_DEMARRAGE.md).**

Plateforme data pour **WestAfrica BancAssur Group**, présent dans 8 pays
(CI, SN, ML, BF, GN, TG, BJ, GH).

- **Level 1 — Ingestion & Lakehouse batch :**
  - une application **Streamlit** génère des données financières synthétiques ;
  - elles sont déposées dans **MinIO** ;
  - **Spark** les valide et les charge dans des tables **Apache Iceberg** ;
  - **Trino** les expose en SQL.
- **Level 2 — Orchestration & Médaillon :**
  - **Apache Airflow 3** orchestre la chaîne Bronze → Silver → Gold ;
  - la couche Gold contient **7 tables de KPIs** financiers et réglementaires ;
  - un reporting **BCEAO / CIMA** est produit chaque jour à J+1.
- **Level 3 — Pipeline hybride batch + streaming (architecture Lambda) :**
  - **Apache NiFi** surveille `raw-landing` et publie chaque ligne CSV en JSON dans **Kafka** ;
  - **Spark Structured Streaming** nettoie / enrichit en continu (Job 1) puis détecte
    fraudes, AML et tensions de liquidité (Job 2) ;
  - **Trino** interroge dans une même requête l'historique Iceberg et les topics Kafka.
- **Level 4 — Production-grade (Kubernetes) :**
  - toute la plateforme sur **Minikube** en une commande (`kubectl apply -k .`), répartie
    sur 5 namespaces, avec Secrets hors git, probes, PVC et Ingress TLS ;
  - **Superset** : 3 dashboards filtrables par pays ;
  - **Keycloak** : SSO pour Superset et Trino, 4 rôles appliqués dans Trino (filtre pays,
    masques de colonnes, tables réglementaires) ;
  - **OpenMetadata** : tables Gold documentées, PII tagués, lineage raw → Gold ;
  - **Prometheus, Loki, Grafana** : dashboard de santé et 3 alertes testées.

```
 Streamlit ──CSV──► MinIO raw-landing
                        │  dag_ingest_raw (toutes les 15 min, détection de fichiers)
                        ▼
                  bronze.*  (brut validé, MERGE idempotent)          ┐
                        │  asset → dag_bronze_to_silver                │  Iceberg (REST catalog)
                        ▼                                              │  sur MinIO lakehouse
                  silver.*  (dédupliqué, enrichi, EUR, pseudonymisé)  │
                        │  asset → dag_silver_to_gold                  │  requêtable
                        ▼                                              │  via Trino
                  gold.*    (7 KPIs + reporting BCEAO/CIMA J+1)        ┘
                        └──► MinIO regulatory-reports (CSV de déclaration)

 Level 3 (speed layer), en parallèle du batch :
 raw-landing ─► NiFi (ListS3 → FetchS3Object → CSV→JSON + ingestion_timestamp/source_file)
            ─► Kafka raw-* ─► Spark Job 1 ─► silver-* (Kafka) + silver.rt_* (Iceberg)
                                   └─► dlq-financial-events (rejets, orphelins)
            silver-* ─► Spark Job 2 ─► gold-fraud-alerts / gold-aml-events / gold-liquidity-alerts
                                        + gold.fraud_alerts / aml_events / liquidity_alerts (Iceberg)
 Trino : gold.* (Iceberg)  ⟗  kafka.default."silver-bank-transactions"  → requête Lambda
```

| Service | URL | Identifiants |
|---|---|---|
| Streamlit (générateur) | http://localhost:8501 | — |
| **Airflow** | http://localhost:**8090** | `admin` / `AIRFLOW_ADMIN_PASSWORD` du `.env` |
| Console MinIO | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` du `.env` |
| Spark master (UI) | http://localhost:8080 | — |
| **Trino (SQL)** | http://localhost:**8088** | utilisateur libre (ex. `admin`), sans mot de passe |
| Catalogue Iceberg REST | http://localhost:8181 | — |
| **NiFi** (L3) | https://localhost:**8443**/nifi | `NIFI_USERNAME` / `NIFI_PASSWORD` du `.env` (certificat auto-signé : accepter l'alerte du navigateur) |
| **Kafka UI** (L3) | http://localhost:**8084** | — (exposé sur 127.0.0.1 uniquement) |
| Kafka (clients hôte) | `localhost:29092` | — |

> ⚠️ Trino écoute sur **8088** et Airflow sur **8090**. Le port 8080 est l'interface web de Spark.

Documentation technique :
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) (Level 1) ·
[`docs/ARCHITECTURE_L2.md`](docs/ARCHITECTURE_L2.md) (Level 2) ·
[`docs/ARCHITECTURE_L3.md`](docs/ARCHITECTURE_L3.md) (Level 3) ·
[`docs/ARCHITECTURE_L4.md`](docs/ARCHITECTURE_L4.md) (Level 4) ·
**write-up technique** : [`docs/WRITEUP.md`](docs/WRITEUP.md) ·
variables d'environnement : [`README.env.example`](README.env.example)

---

## Sommaire
1. [Prérequis](#1-prérequis)
2. [Installation from scratch](#2-installation-from-scratch)
3. [Level 1 : pipeline d'ingestion](#3-level-1--pipeline-dingestion)
4. [Level 2 : pipeline orchestré par Airflow](#4-level-2--pipeline-orchestré-par-airflow)
4 bis. [Level 3 : streaming et architecture Lambda](#4-bis-level-3--streaming-et-architecture-lambda)
4 ter. [Level 4 : Kubernetes, gouvernance, observabilité](#4-ter-level-4--kubernetes-gouvernance-observabilité)
5. [Vérifier les critères d'évaluation](#5-vérifier-les-critères-dévaluation)
6. [Mettre à niveau un déploiement Level 1](#6-mettre-à-niveau-un-déploiement-level-1-existant)
7. [Se connecter avec DBeaver](#7-se-connecter-à-trino-avec-dbeaver)
8. [Commandes utiles](#8-commandes-utiles)
9. [Structure du dépôt](#9-structure-du-dépôt)
10. [Dépannage](#10-dépannage)

---

## 1. Prérequis

| Outil | Version | Vérification |
|---|---|---|
| Docker Desktop (macOS / Windows) ou Docker Engine (Linux) | 24+ | `docker --version` |
| Docker Compose v2 | 2.20+ | `docker compose version` |
| `make`, `bash`, `python3` | — | `make --version` |
| Compte Docker Hub gratuit | — | `docker login` (voir 2.2) |

**Ressources à allouer à Docker** (Docker Desktop → *Settings → Resources*) :
- **10 Go de RAM recommandés** pour le Level 2 (8 Go au minimum) ;
- 4 CPU ;
- environ 20 Go de disque libre.

**Systèmes testés :** macOS Apple Silicon (arm64) et Linux x86_64. Sous **Windows**,
exécute toutes les commandes dans **WSL2 (Ubuntu)**.

**Ports utilisés :** 8501, 8090, 9000, 9001, 8080, 8081, 7077, 8088, 8181.

**Level 4 (Kubernetes) — prérequis supplémentaires :**

| Outil | Version | Vérification |
|---|---|---|
| Minikube | 1.36+ | `minikube version` |
| kubectl (Kustomize intégré) | 1.32+ | `kubectl version --client` |
| `openssl` (certificat TLS local) | — | `openssl version` |
| *Optionnel :* kubeconform, promtool | — | validation hors cluster (`make test-l4`) |

Ressources : `make k8s-up` crée un profil Minikube dédié, **`waba`**, sans toucher à un
éventuel profil `minikube` existant. Il lui donne toute la mémoire de Docker moins 1 Go
(24 Go au plus).

| Mémoire Docker Desktop | Profil appliqué automatiquement | Réservations mémoire |
|---|---|---|
| ≥ 21 Go | `full` | ~18 Go |
| 12 à 20 Go (ex. 16 Go) | `light` : tas JVM et limites réduits, Kafka UI désactivé | ~11,5 Go |
| < 12 Go | insuffisant : augmenter la mémoire dans *Docker Desktop → Settings → Resources* | — |

Réglages possibles : `WABA_PROFILE=full|light`, `MINIKUBE_MEMORY=14g`, `MINIKUBE_CPUS=6`,
`MINIKUBE_DISK=60g`, `K8S_VERSION=v1.33.1`. Par défaut, la version de Kubernetes est la
version `stable` de ton Minikube.

---

## 2. Installation from scratch

### 2.1 Code et variables d'environnement

```bash
git clone <url-du-depot> waba-lakehouse && cd waba-lakehouse
cp .env.example .env
```

Les valeurs d'exemple suffisent pour un test local. **Aucun secret n'est écrit dans le
code** : tout vient de `.env`, qui n'est jamais commité.

| Variable | Rôle |
|---|---|
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | Compte admin MinIO (initialisation et console). Mot de passe de 8 caractères minimum |
| `LAKEHOUSE_S3_ACCESS_KEY` / `LAKEHOUSE_S3_SECRET_KEY` | Compte de service à droits restreints, utilisé par Spark, Trino, Streamlit, Iceberg REST et Airflow |
| `AWS_REGION` | Région S3 exigée par les SDK |
| `PII_HASH_SECRET` | Secret de pseudonymisation des identifiants clients et comptes (Silver / Gold) |
| `AIRFLOW_ADMIN_PASSWORD` | Mot de passe du compte `admin` d'Airflow |
| `AIRFLOW_DB_PASSWORD` | Base PostgreSQL d'Airflow |
| `AIRFLOW_FERNET_KEY`, `AIRFLOW_JWT_SECRET`, `AIRFLOW_API_SECRET_KEY` | Secrets internes d'Airflow. Commande de génération dans `.env.example` |
| `ALERT_WEBHOOK_URL` | Optionnel : webhook Slack / Teams appelé en cas d'échec d'une tâche |
| `WABA_INGEST_SCHEDULE`, `SPARK_POOL_SLOTS` | Fréquence d'ingestion (cron) et nombre de jobs Spark simultanés |
| `COMPOSE_PROFILES=streaming` | (L3) démarre aussi Kafka, NiFi, Kafka UI et les jobs streaming. Vide = L1 + L2 seulement |
| `NIFI_USERNAME`, `NIFI_PASSWORD`, `NIFI_SENSITIVE_PROPS_KEY` | (L3) compte NiFi et clé de chiffrement du flux — **12 caractères minimum** |
| `NIFI_REPLAY_HISTORY` | (L3) `false` : NiFi ne publie que les fichiers déposés après son démarrage ; `true` : rejoue tout `raw-landing` |

### 2.2 Docker Hub

Docker Hub limite les téléchargements anonymes (erreur `429 Too Many Requests`) :

```bash
docker login
```

### 2.3 Télécharger les images de base (une par une)

```bash
docker pull pgsty/silo:RELEASE.2026-08-06T00-00-00Z
docker pull pgsty/mc:RELEASE.2026-09-16T00-00-00Z
docker pull tabulario/iceberg-rest:1.6.0
docker pull trinodb/trino:460
docker pull apache/spark:3.5.3-scala2.12-java17-python3-ubuntu
docker pull python:3.11-slim
docker pull apache/airflow:3.3.2
docker pull postgres:16-alpine
docker pull tecnativa/docker-socket-proxy:v0.4.2
# Level 3
docker pull apache/kafka:4.3.1
docker pull kafbat/kafka-ui:v1.5.0
docker pull apache/nifi:2.12.0
docker pull python:3.12-alpine
```

> **Pourquoi `pgsty/silo` et pas `minio/minio` ?** MinIO ne publie plus ses images depuis
> septembre 2026 : l'image a été retirée de Docker Hub et `quay.io` n'accepte plus les
> téléchargements anonymes. `pgsty/silo` est une version de MinIO maintenue par la
> communauté, avec la même API S3.

### 2.4 Construire et démarrer

```bash
docker compose up -d --build        # ou : make up
docker compose ps
```

Le premier build prend 10 à 20 minutes (JAR Iceberg, paquets Python). Résultat attendu :

| Service | État attendu |
|---|---|
| `minio` | `Up (healthy)` |
| `minio-init`, `airflow-init` | `Exited (0)` : c'est normal, ce sont des tâches d'initialisation |
| `iceberg-rest`, `spark-master`, `spark-worker`, `docker-proxy`, `airflow-db`, `airflow-dag-processor` | `Up` |
| `trino`, `generator`, `airflow-apiserver`, `airflow-scheduler` | `Up (healthy)`, après 30 à 60 s |
| (L3) `kafka` | `Up (healthy)` |
| (L3) `kafka-init`, `nifi-init` | `Exited (0)` (topics créés, flux NiFi provisionné — `nifi-init` attend que NiFi soit prêt, 1 à 3 min) |
| (L3) `nifi`, `kafka-ui`, `stream-silver`, `stream-gold` | `Up` (les jobs streaming redémarrent seuls tant que le Level 2 n'a pas encore produit `silver.fx_rates` / `silver.accounts`) |

> Mémoire : avec le Level 3, prévoir **14 à 16 Go** pour Docker (NiFi 2 Go, Kafka 1 Go,
> 2 jobs streaming de 1,5 Go). Sur une petite machine : `COMPOSE_PROFILES=` dans `.env`
> pour travailler sur L1/L2, puis `make streaming-up` à la demande.

### 2.5 Créer les zones Iceberg

```bash
make init
```

Cette commande crée les schémas `bronze`, `silver`, `gold` et `audit`, ainsi que les 8
tables Bronze.

```bash
docker compose exec trino trino --execute "SHOW SCHEMAS FROM iceberg"
# -> audit, bronze, gold, information_schema, silver (+ raw si Level 1 déjà déployé)
```

---

## 3. Level 1 : pipeline d'ingestion

### 3.1 Générer les données

**Via Streamlit** (http://localhost:8501) :
1. Choisis **Référentiels**, mode **One-time**, puis clique sur *Générer*. Cette étape doit
   **toujours** précéder les transactions.
2. Puis, pour **Transactions bancaires**, **Assurance**, **Mobile money** et
   **Remboursements de crédit** : choisis les pays, la période (par défaut le dernier
   trimestre) et *Anomalies* à 1 %, puis clique sur *Générer*.
3. **Mode continu** : un micro-lot est produit toutes les 10 à 60 s.

**Ou en ligne de commande :**

```bash
make generate-referentials          # machine modeste : CUSTOMERS=50000 ACCOUNTS=80000
make generate-events                # 10k bancaire, 5k assurance, 20k mobile money, 5k crédits
```

### 3.2 Ingestion manuelle et contrôles

```bash
make ingest          # raw-landing -> bronze.* (validation + MERGE idempotent + archivage)
make quality         # 0 orphelin, 0 doublon, 0 incohérence de devise, motifs de rejet
make analytics       # soldes par pays, volumes, comptages par entité
make idempotency     # rejoue toute l'archive : les comptages ne bougent pas
```

Au Level 2, cette ingestion est faite automatiquement par Airflow (voir section 4).

---

## 4. Level 2 : pipeline orchestré par Airflow

### 4.1 L'interface Airflow

Ouvre http://localhost:8090 et connecte-toi avec `admin` et le `AIRFLOW_ADMIN_PASSWORD` du
`.env`. Tu y trouves 4 DAGs, activés par défaut :

| DAG | Déclenchement | Rôle |
|---|---|---|
| `dag_ingest_raw` | toutes les 15 min | Détecte les nouveaux fichiers dans MinIO et charge Bronze pour les 8 pays. Si aucun fichier n'est trouvé, la chaîne s'arrête (short-circuit) |
| `dag_bronze_to_silver` | après `dag_ingest_raw`, via l'asset `bronze` | Nettoyage, déduplication, enrichissement, conversion EUR, pseudonymisation |
| `dag_silver_to_gold` | après `dag_bronze_to_silver`, via l'asset `silver` | Calcule les 7 tables de KPIs |
| `dag_regulatory_report` | chaque jour à 00h30 UTC | Agrégats BCEAO / CIMA de la veille (J+1) et fichiers de déclaration |

Les jobs Spark s'exécutent dans des conteneurs éphémères créés par Airflow. Leurs logs
apparaissent directement dans l'onglet *Logs* de chaque tâche.

### 4.2 Lancer la chaîne complète

Avec des données dans `raw-landing` (étape 3.1) :

```bash
make airflow-pipeline
```

Cette commande déclenche `dag_ingest_raw`, puis attend `dag_bronze_to_silver` et
`dag_silver_to_gold`. Compte 5 à 10 minutes. Tu peux aussi attendre le prochain quart
d'heure, ou cliquer sur ▶ *Trigger* dans l'interface.

**Paramétrage par pays :** déclenche le DAG avec une configuration, depuis l'interface
(*Trigger* → `countries`) ou en ligne de commande :

```bash
docker compose exec airflow-apiserver airflow dags trigger dag_silver_to_gold \
  --conf '{"countries": ["CI", "SN"]}'
```

La liste par défaut vient de la Variable Airflow `waba_countries`. Seules les partitions
des pays demandés sont recalculées.

### 4.3 Reporting réglementaire J+1

Le DAG tourne chaque nuit pour la veille. Or les données générées couvrent le trimestre
précédent (avril à juin 2026) : pour la démo, rejoue une date qui en fait partie.

```bash
make airflow-regulatory
make regulatory-sql                  # déclarations BCEAO et CIMA
```

Les fichiers de déclaration se trouvent dans la console MinIO, sous
`regulatory-reports/bceao/report_date=<date>/country_code=CI/...`.

### 4.4 Consulter les résultats

```bash
make medallion       # 3 zones, partitions par pays, unicité, taux EUR, pseudonymisation
make kpis            # les 7 KPIs Gold
```

Exemple avec le NPL par pays (seuil BCEAO de 5 %) :

```sql
SELECT country_code, round(npl_ratio * 100, 2) AS npl_pct, is_above_threshold
FROM iceberg.gold.npl_ratio_by_country
WHERE loan_type = 'ALL' ORDER BY country_code;
```

### 4.5 Sans Airflow (débogage)

Les mêmes jobs peuvent être lancés à la main :

```bash
make ingest
make silver COUNTRIES="CI SN"
make gold
make regulatory
```

### 4.6 Tout en une commande

```bash
make up && make demo-l2
```

Cette commande enchaîne `init`, génération, pipeline Airflow, reporting, vérifications et
KPIs.

---

## 4 bis. Level 3 : streaming et architecture Lambda

Pré-requis : le Level 2 a tourné au moins une fois (`make airflow-pipeline`). Les jobs
streaming utilisent les référentiels Bronze, `silver.fx_rates` et `silver.accounts`.

### 4b.1 Démarrer la couche temps réel

```bash
make env-upgrade        # ajoute NIFI_* et COMPOSE_PROFILES=streaming à un .env existant
# éditer .env : NIFI_PASSWORD et NIFI_SENSITIVE_PROPS_KEY (12 caractères minimum)
make build-spark        # l'image Spark embarque maintenant les JAR Kafka
make streaming-up       # kafka, kafka-init, kafka-ui, nifi, nifi-init, stream-silver, stream-gold
docker compose logs nifi-init     # {"status": "CREATED", ...} quand le flux est en place
```

Dans NiFi (https://localhost:8443/nifi), le groupe **« WABA - raw-landing -> Kafka »** est
démarré : `ListS3` (toutes les 5 s) → `RouteOnAttribute` (seuls les 4 flux événementiels)
→ `FetchS3Object` → `UpdateAttribute` (topic `raw-<flux>`) → `UpdateRecord` (CSV → JSON,
une ligne = un événement, + `ingestion_timestamp`, `source_file`, `landed_at`) →
`PublishKafka`. Toutes les connexions ont un **back-pressure de 10 000 flowfiles / 1 Go**.

Par défaut NiFi ne publie que les fichiers déposés **après** son provisioning (l'historique
du trimestre reste traité par le batch). Pour tout rejouer :
`NIFI_REPLAY_HISTORY=true` dans `.env` puis `make nifi-provision NIFI_RECREATE=true`.

### 4b.2 Générer des événements et des fraudes

```bash
make fraud-demo         # 5 scénarios horodatés « maintenant » pour CI et GH
make dlq-demo           # un fichier volontairement défectueux (4 lignes invalides)
```

Les scénarios sont aussi disponibles dans Streamlit (encadré **« 🚨 Level 3 »**), et le
mode *continu* de Streamlit alimente le flux en permanence.

| Scénario | Alerte attendue | Topic / table |
|---|---|---|
| `burst` : 3 virements > 500 000 XOF du même compte en < 2 min | `MULTIPLE_LARGE_TXN` | `gold-fraud-alerts` / `gold.fraud_alerts` |
| `unusual_country` : paiement mobile émis hors du pays du client | `UNUSUAL_COUNTRY` | idem |
| `big_claim` : sinistre = 10 × les primes de la police | `CLAIM_EXCEEDS_PREMIUM` | idem |
| `aml` : virements > 1 000 000 XOF / > 5 000 GHS | `AML_THRESHOLD_EXCEEDED` | `gold-aml-events` / `gold.aml_events` |
| `bank_run` : 30 retraits = 80 % de la réserve de liquidité du pays | `LIQUIDITY_COVERAGE_BREACH` | `gold-liquidity-alerts` / `gold.liquidity_alerts` |

### 4b.3 Observer

```bash
make stream-logs        # logs JSON par micro-lot : rows_in, silver_rows, dlq, max_lag_s, alertes
make topics             # offsets (nombre de messages) de chaque topic
make streaming-check    # SQL : lag NiFi→Kafka, fraîcheur silver.rt_*, alertes par règle, DLQ
make dlq                # 20 derniers messages de dlq-financial-events
make lambda             # requête Lambda de l'énoncé (Iceberg Gold ⟗ Kafka) + variante agrégée
```

Kafka UI (http://localhost:8084) permet de lire les messages de chaque topic.

---

## 4 ter. Level 4 : Kubernetes, gouvernance, observabilité

Tout ce qui tournait en docker compose est redéployé sur Kubernetes, et 3 briques
s'ajoutent : Superset, Keycloak et OpenMetadata, plus la stack
Prometheus / Loki / Grafana. Détails et choix : [`docs/ARCHITECTURE_L4.md`](docs/ARCHITECTURE_L4.md).

### 4t.1 Déployer toute la plateforme

```bash
cp .env.example .env            # si ce n'est pas déjà fait (Levels 1-3)
make k8s-up
```

`make k8s-up` enchaîne :
1. `minikube -p waba start` (runtime containerd, mémoire adaptée à Docker) et l'addon `ingress` ;
2. la construction des 5 images du projet dans Minikube ;
3. la génération des secrets (`k8s/secrets.env`, mots de passe aléatoires, et
   `k8s/.generated/`, tous deux ignorés par git) ;
4. **une seule commande** pour toute la plateforme :

```bash
kubectl apply --server-side -k .
```

Premier démarrage : 10 à 20 minutes (téléchargement des images). Suivi :

```bash
make k8s-status                 # pods des 5 namespaces, Jobs d'initialisation, Ingress
make k8s-hosts                  # ligne à ajouter à /etc/hosts (sudo)
make k8s-credentials            # identifiants générés (SSO, consoles)
```

> **macOS (driver docker) :** laisse `minikube -p waba tunnel` tourner dans un autre terminal, et
> fais pointer les noms `*.waba.local` sur `127.0.0.1` (`make k8s-hosts` l'indique).

| Service | URL (certificat auto-signé : accepter l'alerte) |
|---|---|
| Superset (SSO Keycloak) | https://superset.waba.local |
| Trino (UI, SSO Keycloak) | https://trino.waba.local/ui/ |
| Keycloak (console) | https://keycloak.waba.local/admin |
| OpenMetadata | https://openmetadata.waba.local |
| Grafana | https://grafana.waba.local |
| Airflow | https://airflow.waba.local |
| NiFi | https://nifi.waba.local/nifi |
| MinIO / Streamlit | https://minio.waba.local · https://streamlit.waba.local |
| Kafka UI (sans authentification) | `kubectl -n ingestion port-forward svc/kafka-ui 8084:8080` |

**Comptes de démonstration** (mot de passe commun : `WABA_DEMO_PASSWORD`, affiché par
`make k8s-credentials`) :

| Utilisateur | Rôle | Ce qu'il voit |
|---|---|---|
| `admin.groupe` | group_admin | Tout |
| `analyste.ci` / `analyste.gh` | country_analyst | Uniquement les lignes de son pays (filtre appliqué par Trino) |
| `conformite` | compliance_officer | Dashboard Risque & Conformité ; tables Gold réglementaires uniquement |
| `lecteur` | viewer | Agrégats, identifiants masqués (`***`), pas de montants de sinistres individuels |

### 4t.2 Données, pipeline et temps réel dans le cluster

```bash
make k8s-data                    # référentiels (50 000 clients) + événements du trimestre
make k8s-pipeline                # Airflow : ingest -> silver -> gold, jobs via le Spark Operator
make k8s-regulatory            # reporting BCEAO / CIMA J+1
make k8s-fraud                   # scénarios de fraude / AML / liquidité (Level 3)
make k8s-governance              # ingestion OpenMetadata + documentation, PII, lineage
make k8s-superset                # (ré)import des dashboards après le premier pipeline
```

Suivre les jobs Spark batch : `kubectl -n processing get sparkapplications`.

### 4t.3 Déclencher les 3 alertes Grafana

```bash
make k8s-alert-demo A=regulatory   # dag_regulatory_report en échec     -> dès l'échec du DAG
make k8s-alert-demo A=lag          # lag du consommateur AML > 5 000    -> ~2 min
make k8s-alert-demo A=fraud-down   # job de fraude arrêté > 5 min       -> ~6 min
make k8s-alert-demo A=restore      # retour à la normale
kubectl -n monitoring logs deploy/alert-webhook   # notifications reçues
```

Dans Grafana : *Alerting → Alert rules* (dossier WABA) pour l'état et l'historique,
*Dashboards → WABA → Santé des pipelines* pour le suivi.

---

## 5. Vérifier les critères d'évaluation

### Level 1

| Critère | Vérification | Attendu |
|---|---|---|
| Streamlit (2 modes × 4 types) | http://localhost:8501 | Fichiers dans `raw-landing/<CC>/<dataset>/` |
| Cohérence référentielle | `make quality` | 0 clé orpheline |
| Multi-pays | `make trino-sql FILE=01_exploration.sql` | `country_code` et `entity_type` partout ; partitions par pays |
| Tables Iceberg peuplées | idem | 8 tables avec des lignes (`bronze.*`, ex-`raw.*`) |
| Idempotence | `make idempotency` | Comptages identiques avant et après rejeu |
| Docker Compose | `docker compose up -d --build` | Toute la stack démarre avec une seule commande |

### Level 2

| Critère | Vérification | Attendu |
|---|---|---|
| **4 DAGs opérationnels** | Airflow http://localhost:8090, puis `make airflow-pipeline` et `make airflow-regulatory` | Runs en `success` ; dépendances visibles dans *Graph* et *Assets* ; paramètre `countries` |
| **Médaillon** | `make medallion` (requêtes 1 et 3) | Schémas `bronze`, `silver` et `gold` avec des volumes distincts, partitionnés par `country_code` |
| **Silver de qualité** | `make medallion` (requêtes 2 et 4 à 7) | 0 doublon, montants en EUR, aucun identifiant en clair, quarantaine des orphelins, aberrants signalés |
| **7 KPIs Gold** | `make kpis` | Les 7 tables répondent ; filtre `WHERE country_code = 'CI'` ; NPL entre 3 et 8 %, loss ratio entre 50 et 85 % par pays |
| **Aucun credential en dur** | `grep -rn "change-me" airflow/ spark/ generator/` (aucun résultat) ; Airflow → *Admin → Connections / Variables* | Connection `minio_s3` et Variables injectées par l'environnement |
| **Reporting J+1** | `make airflow-regulatory`, puis `make regulatory-sql` | Tables `gold.regulatory_*` et CSV dans `regulatory-reports/` ; cron `30 0 * * *` UTC |

### Level 3

| Critère | Vérification | Attendu |
|---|---|---|
| **NiFi → Kafka** | `make fraud-demo`, puis `make streaming-check` (requête 1) | Messages dans les 4 topics `raw-*`, `max_lag_s` < 30 |
| **Job 1** | `make streaming-check` (requête 2), `make stream-logs` | `silver-*` alimentés et `silver.rt_*` mis à jour à chaque micro-lot (5 s) |
| **Fraude** | `make streaming-check` (requêtes 3 et 4) | Les 3 règles `MULTIPLE_LARGE_TXN`, `UNUSUAL_COUNTRY`, `CLAIM_EXCEEDS_PREMIUM` dans `gold-fraud-alerts` |
| **AML** | idem | `AML_THRESHOLD_EXCEEDED` en XOF et en GHS dans `gold-aml-events` |
| **DLQ** | `make dlq-demo`, puis `make dlq` / requête 5 | `MALFORMED_ROW`, `CURRENCY_COUNTRY_MISMATCH`, `ORPHAN_ACCOUNT`, `MISSING_*` dans `dlq-financial-events` |
| **Requête Lambda** | `make lambda` | La requête de l'énoncé s'exécute sans erreur |

### Level 4

| Critère | Vérification | Attendu |
|---|---|---|
| **Stack K8s déployable** | `make k8s-up` (une seule commande `kubectl apply -k .`), puis `make k8s-status` | Pods `Running`/`Ready` dans `ingestion`, `processing`, `serving`, `governance`, `monitoring` ; Jobs `Complete` |
| **Superset, 3 dashboards** | https://superset.waba.local (connexion Keycloak `admin.groupe`) | *Performance commerciale*, *Risque & conformité*, *Mobile money* avec des données Trino ; filtre natif « Pays » |
| **SSO Keycloak, 4 rôles** | Se connecter avec `analyste.ci`, `conformite`, `lecteur` ; UI Trino https://trino.waba.local/ui/ | Redirection vers Keycloak ; `analyste.ci` ne voit que CI ; `conformite` n'accède qu'aux tables réglementaires ; `lecteur` voit `***` à la place des identifiants |
| **OpenMetadata : lineage et PII** | `make k8s-governance`, puis https://openmetadata.waba.local | Tables `gold.*` documentées (description, propriétaire, tags BCEAO/CIMA, glossaire) ; onglet *Lineage* raw-landing → bronze → silver → gold ; colonnes `*_id` / `*_key` en `PII.Sensitive` |
| **Métriques et logs** | Grafana → *WABA — Santé des pipelines* ; *Explore → Loki* `{namespace="processing"} \| json` | Latence par pays, lag Kafka, débit NiFi, erreurs Airflow ; logs JSON des jobs Spark analysés |
| **Alertes testées** | `make k8s-alert-demo A=...` ; *Alerting → Alert rules* | Les 3 alertes passent en *Firing* ; notifications dans `alert-webhook` |

Tests unitaires et validations hors cluster :

```bash
make test            # générateur 19 + Spark 49 (dont 12 streaming et métriques) + DAGs 13
make test-nifi       # provisioning NiFi contre un faux serveur REST (2)
make test-l4         # Superset, OpenMetadata, identité, alertes (promtool), manifestes
```

Les tests streaming tournent **sans Kafka** : vraie exécution Structured Streaming (source
fichiers, déduplication 10 min, fenêtre glissante répartie sur deux micro-lots) et un test
bout en bout *scénarios du générateur → Job 1 → JSON silver-* → Job 2 → alertes*.

---

## 6. Mettre à niveau un déploiement Level 1 existant

```bash
make env-upgrade          # ajoute à .env les nouvelles variables (sans toucher aux existantes)
docker compose up -d --build
make init                 # crée bronze / silver / gold
make bootstrap-bronze     # recharge en Bronze les fichiers déjà archivés au Level 1
make airflow-pipeline     # Bronze -> Silver -> Gold
```

Si tu avais généré les référentiels avant cette version, régénère-les une fois. Le
générateur est désormais calibré pour produire des NPL et des loss ratios réalistes.

```bash
make generate-referentials CUSTOMERS=50000 ACCOUNTS=80000 && make generate-events
```

---

### Passer du Level 2 au Level 3

```bash
make env-upgrade && vi .env        # renseigner NIFI_PASSWORD / NIFI_SENSITIVE_PROPS_KEY
make build-spark
# gold.daily_transaction_volume a de nouveaux noms de colonnes (txn_date, total_amount_eur)
# pour que la requête Lambda de l'énoncé s'exécute telle quelle : recréer la table.
./scripts/trino.sh -e "DROP TABLE iceberg.gold.daily_transaction_volume"
make airflow-pipeline              # ou : make silver gold
make streaming-up
```

### Passer au Level 4

Le code des Levels 1-3 a évolué (métriques Prometheus des jobs streaming, backend
Spark Operator d'Airflow) sans changer le fonctionnement en docker compose. Pour que
`make test` passe dans les conteneurs, reconstruire les images Spark et Airflow :

```bash
docker compose build spark-master airflow-apiserver && docker compose up -d
```

Le déploiement Kubernetes est indépendant de docker compose (autres volumes, autres
données) : `make k8s-up`, puis `make k8s-data k8s-pipeline`.

---

## 7. Se connecter à Trino avec DBeaver

1. Crée une *Nouvelle connexion* de type **Trino**, avec : Host `localhost`, Port
   **`8088`**, Database `iceberg`, utilisateur `admin`, mot de passe vide.
2. Vérifie que l'URL JDBC est `jdbc:trino://localhost:8088/iceberg`.

Si tu obtiens l'erreur `405 ... org.apache.spark.ui`, la connexion vise le port 8080, celui
de Spark.

---

## 8. Commandes utiles

`make help` liste toutes les commandes. Les principales :

| Commande | Effet |
|---|---|
| `make up` / `make down` / `make clean` | Démarrer / arrêter / tout supprimer (volumes inclus) |
| `make init` | Schémas et tables Bronze |
| `make generate-referentials` / `make generate-events` | Génération en ligne de commande |
| `make airflow-pipeline` | Ingestion → Silver → Gold via Airflow (déclenche et attend) |
| `make airflow-regulatory D=...` | Reporting J+1 pour une date |
| `make airflow-status` | Derniers runs des 4 DAGs |
| `make ingest` / `make silver` / `make gold` / `make regulatory` | Jobs lancés à la main, sans Airflow |
| `make medallion` / `make kpis` / `make regulatory-sql` | Vérifications SQL du Level 2 |
| `make quality` / `make analytics` / `make idempotency` | Vérifications du Level 1 |
| `make build-spark` | Reconstruire l'image Spark après une modification des jobs (utilisée par Airflow) |
| `make test` | Tests unitaires |
| `make streaming-up` / `make nifi-provision` | (L3) démarrer le temps réel / recréer le flux NiFi |
| `make fraud-demo` / `make dlq-demo` | (L3) injecter fraudes, AML, bank run / un fichier défectueux |
| `make stream-logs` / `make topics` / `make dlq` | (L3) observer les jobs et les topics |
| `make streaming-check` / `make lambda` | (L3) contrôles SQL et requête Lambda |

---

## 9. Structure du dépôt

```
.
├── kustomization.yaml          # L4 : TOUTE la plateforme Kubernetes (kubectl apply -k .)
├── README.env.example          # toutes les variables d'environnement (L1 à L4), valeurs d'exemple
├── docker-compose.yml          # stack complète L1 + L2 (+ profil « streaming » : L3)
├── .env.example                # variables d'environnement (valeurs d'exemple)
├── Makefile                    # raccourcis (make help)
├── generator/                  # Streamlit + CLI de génération (calibrée NPL / loss ratio)
├── spark/
│   ├── Dockerfile              # Spark 3.5.3 + Iceberg 1.6.1 (image utilisée aussi par Airflow)
│   ├── jobs/
│   │   ├── ingest.py           #   raw-landing -> bronze.* (ou raw.* au L1)
│   │   ├── silver.py           #   bronze -> silver
│   │   ├── gold.py             #   silver -> gold (7 KPIs)
│   │   ├── regulatory_report.py#   reporting BCEAO / CIMA J+1
│   │   ├── stream_raw_to_silver.py  # L3 Job 1 : raw-* -> silver-* + silver.rt_* + DLQ
│   │   ├── stream_silver_to_gold.py # L3 Job 2 : fraude, AML, liquidité
│   │   └── common/             #   schémas, validation, fx, transforms, streaming, stream_pipeline
│   └── tests/                  #   validation Bronze, médaillon, streaming
├── airflow/
│   ├── Dockerfile, init.sh     # Airflow 3.3.2 (providers docker + amazon), pool, admin
│   ├── dags/                   # 4 DAGs + waba/common.py (SparkJobOperator, alertes, pays)
│   └── tests/                  # tests des DAGs
├── minio/                      # init : buckets, versioning, compte de service
├── k8s/                        # L4 : manifestes par namespace (ingestion, processing, serving,
│   │                           #      governance, monitoring), users.json, secrets.env.example
│   ├── processing/spark-operator/  # CRD + contrôleur Kubeflow Spark Operator 2.5.2
│   ├── serving/trino/          # config Trino (TLS, OAuth2, règles d'accès générées)
│   ├── governance/keycloak/    # realm waba (généré)
│   └── monitoring/             # Prometheus, Loki/Promtail, Grafana (dashboard, alertes, tests)
├── superset/                   # L4 : image, superset_config.py (SSO), bundle des 3 dashboards
├── governance/                 # L4 : script OpenMetadata (docs, glossaire, PII, lineage) + tests
├── nifi/                       # L3 : provision.py (flux NiFi par API REST) + tests
├── kafka/                      # L3 : create-topics.sh (12 topics)
├── trino/etc/                  # catalogues iceberg + kafka (kafka/*.json : colonnes des topics)
├── sql/                        # 01-03 L1, 04-06 L2, 07 Lambda, 08 contrôles streaming
├── scripts/                    # spark-submit, trino, airflow_run, env_upgrade, idempotence
│   └── k8s/                    # L4 : up, images, secrets, identité, contrôles, démo d'alertes
└── docs/                       # ARCHITECTURE*.md (L1 à L4), WRITEUP.md (write-up technique)
```

---

## 10. Dépannage

| Symptôme | Cause | Solution |
|---|---|---|
| `required variable ... is missing a value` au `docker compose up` | `.env` du Level 1 sans les variables du Level 2 | `make env-upgrade` |
| `pull access denied for minio/minio` ou `401` sur `quay.io/minio` | Images officielles MinIO retirées | Le dépôt utilise `pgsty/silo` / `pgsty/mc` |
| `429 Too Many Requests` | Limite de Docker Hub | `docker login`, puis les `docker pull` un par un (2.3) |
| `ReadTimeoutError` pendant le build | Connexion lente | Relancer `docker compose up -d --build` (les couches déjà construites restent en cache) |
| Tâche Airflow en échec : `Cannot connect to the Docker daemon` / `docker-proxy` | Proxy Docker absent ou socket inaccessible | `docker compose ps docker-proxy` ; sous Linux, vérifier `/var/run/docker.sock` |
| Tâche Spark en échec : `Table not found bronze.*` | Tables pas encore créées | `make init` |
| Tâche Spark bloquée sur « Initial job has not accepted any resources » | RAM Docker insuffisante | 10 Go pour Docker, garder `SPARK_POOL_SLOTS=1` |
| `dag_ingest_raw` en *skipped* | Aucun nouveau fichier dans `raw-landing` | Normal : générer des données, puis relancer |
| Jobs modifiés mais Airflow exécute l'ancienne version | Code intégré à l'image Spark | `make build-spark` |
| Reporting sans transaction | Date hors de la période générée | `make airflow-regulatory` (sans D : la date par défaut tombe dans le trimestre généré) |
| Connexion Airflow refusée | Mauvais mot de passe | `admin` / `AIRFLOW_ADMIN_PASSWORD` ; après modification : `docker compose up -d airflow-init` |
| DBeaver : `405 ... org.apache.spark.ui` | Mauvais port | Trino est sur **8088** |
| (L3) `nifi-init` échoue : `NiFi injoignable` | NiFi met 1 à 3 min à démarrer, ou mot de passe < 12 caractères | `docker compose logs nifi` ; puis `make nifi-provision` |
| (L3) `stream-silver` redémarre en boucle : `silver.fx_rates absente` | Level 2 jamais exécuté | `make airflow-pipeline` ; le job repart tout seul |
| (L3) Aucun message dans `raw-*` après `make fraud-demo` | NiFi ignore les fichiers antérieurs à son démarrage, ou flux arrêté | Vérifier le groupe dans l'UI NiFi (bulletins rouges) ; `make nifi-provision NIFI_RECREATE=true` |
| (L3) `ClassNotFoundException ... kafka` dans les logs Spark | Ancienne image Spark sans JAR Kafka | `make build-spark && make streaming-up` |
| (L3) Trino : `Column 'txn_date' cannot be resolved` dans `make lambda` | Table Gold créée avant le L3 | `DROP TABLE iceberg.gold.daily_transaction_volume` puis `make gold` |
| (L3) Repartir de zéro côté streaming | Checkpoints incohérents après suppression des topics | `docker compose rm -sf stream-silver stream-gold && docker volume rm waba_stream-checkpoints && make streaming-up` |
| (L4) `RSRC_OVER_ALLOC_MEM` au démarrage de Minikube | Plus de mémoire demandée que Docker n'en a | Le script s'adapte désormais tout seul ; sinon `MINIKUBE_MEMORY=12g make k8s-up` |
| (L4) `Kubernetes version ... is newer than the newest supported` | Minikube trop ancien pour la version demandée | Par défaut : version `stable` de ton Minikube ; sinon `K8S_VERSION=v1.33.1` |
| (L4) Pods `Pending` (Insufficient memory) | Mémoire Minikube insuffisante | `make k8s-destroy`, augmenter la mémoire de Docker Desktop, puis `make k8s-up` ; ou mettre temporairement la gouvernance en pause : `kubectl -n governance scale deploy/openmetadata sts/elasticsearch --replicas=0` |
| (L4) `ErrImageNeverPull` / `ImagePullBackOff` sur `waba/*` | Images non construites dans Minikube | `make k8s-images` |
| (L4) `no matches for kind "SparkApplication"` | Application sans `--server-side` (CRD trop volumineuses) | `kubectl apply --server-side -k .` |
| (L4) `stream-silver` / `stream-gold` en `CrashLoopBackOff` | Tables Silver pas encore créées | `make k8s-data k8s-pipeline` ; les pods repartent seuls |
| (L4) Superset : « Invalid parameter: redirect_uri » | Superset servi sous un autre nom d'hôte | Accéder par https://superset.waba.local (entrée /etc/hosts) |
| (L4) Superset : dashboards vides ou erreur Trino | Import fait avant le premier pipeline | `make k8s-pipeline` puis `make k8s-superset` |
| (L4) Job `superset-init` en échec : Trino injoignable | Trino démarre en dernier | Le Job réessaie seul ; sinon `make k8s-superset` |
| (L4) OpenMetadata : tables Gold absentes | Gouvernance lancée avant le pipeline | `make k8s-governance` après `make k8s-pipeline` |
| (L4) Changer un mot de passe | — | Éditer `k8s/secrets.env`, `make k8s-apply`, puis `kubectl rollout restart` du composant. Keycloak ne lit le realm qu'au premier import : changer ensuite les secrets clients / mots de passe dans sa console |
| Tout remettre à zéro | — | `make clean && make up && make init` ; (L4) `make k8s-destroy && make k8s-up` |
