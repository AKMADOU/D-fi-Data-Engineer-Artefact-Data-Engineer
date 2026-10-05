# Guide de démarrage — Levels 1, 2 et 3 de A à Z

Ce guide fait tourner les **3 premiers levels** sur ton ordinateur avec Docker, une
étape après l'autre. Fais les étapes **dans l'ordre**. Chaque étape se termine par un
**✅ Contrôle** : ne passe à la suivante que si le contrôle est bon.

Durée totale : environ **1 heure**, dont 20 à 30 minutes de téléchargement la première fois.

> Les commandes sont écrites pour **macOS**. Sous Linux, une seule change (étape 3) :
> elle est indiquée.

---

## Étape 0 — Ce qu'il te faut

1. **Docker Desktop** installé et **lancé** (l'icône de la baleine est dans la barre du haut).
2. Dans Docker Desktop → ⚙️ *Settings* → *Resources* :
   - **Memory : 12 Go minimum** (16 Go conseillés pour le Level 3)
   - **CPUs : 4** minimum
   - **Disk : 40 Go**

   Clique sur *Apply & restart*.
3. Un compte **Docker Hub** gratuit (https://hub.docker.com). Il évite l'erreur
   « 429 Too Many Requests ».

✅ **Contrôle** : dans un terminal, ces 3 commandes affichent une version, sans erreur.
```bash
docker --version
docker compose version
python3 --version
```

---

## Étape 1 — Récupérer le projet

Clone le dépôt, puis entre dans le dossier :
```bash
git clone https://github.com/<ton-compte>/waba-lakehouse.git
cd waba-lakehouse
ls
```

✅ **Contrôle** : `ls` affiche notamment `Makefile`, `docker-compose.yml` et `README.md`.

> ⚠️ Toutes les commandes suivantes se lancent **depuis ce dossier**.

---

## Étape 2 — Repartir de zéro

Les anciens essais ont laissé des données Docker (MinIO, base Airflow…) créées avec
d'**autres mots de passe**. On les supprime pour éviter les erreurs d'identifiants :
```bash
docker compose down -v --remove-orphans
```

✅ **Contrôle** : la commande se termine sans erreur. Des lignes « Removed » ou rien du
tout, c'est normal.

---

## Étape 3 — Créer le fichier `.env`

Le fichier `.env` contient les mots de passe. Il n'est **jamais** dans le zip ; on le crée
à partir du modèle :
```bash
cp .env.example .env
```

Le streaming (Level 3) sera démarré plus tard. Pour l'instant, on le désactive :
```bash
sed -i '' 's/^COMPOSE_PROFILES=.*/COMPOSE_PROFILES=/' .env      # macOS
# sed -i 's/^COMPOSE_PROFILES=.*/COMPOSE_PROFILES=/' .env       # Linux (à la place)
```

Les valeurs du modèle fonctionnent telles quelles pour un usage local. Tu peux les
changer (`open -e .env`), à condition de garder **au moins 12 caractères** pour
`NIFI_PASSWORD` et `NIFI_SENSITIVE_PROPS_KEY`.

✅ **Contrôle** : ces deux commandes affichent `PII_HASH_SECRET=change-me-pii-secret`
puis `COMPOSE_PROFILES=`.
```bash
grep PII_HASH_SECRET .env
grep COMPOSE_PROFILES .env
```

---

## Étape 4 — Télécharger les images (une par une)

```bash
docker login
```
Entre ton identifiant et ton mot de passe Docker Hub. Puis lance ces commandes **une
par une** (si l'une échoue, relance-la) :
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
docker pull apache/kafka:4.3.1
docker pull kafbat/kafka-ui:v1.5.0
docker pull apache/nifi:2.12.0
docker pull python:3.12-alpine
```

✅ **Contrôle** : chaque commande finit par `Status: Downloaded newer image` ou
`Status: Image is up to date`.

---

## Étape 5 — Démarrer la plateforme

```bash
make up
```
La première fois, la construction prend 10 à 20 minutes. Attends ensuite **2 minutes**,
puis :
```bash
docker compose ps
```

✅ **Contrôle** :

| Service | État attendu |
|---|---|
| `minio`, `trino`, `generator`, `airflow-apiserver`, `airflow-scheduler` | `Up (healthy)` |
| `iceberg-rest`, `spark-master`, `spark-worker`, `airflow-db`, `airflow-dag-processor`, `docker-proxy` | `Up` |
| `minio-init`, `airflow-init` | absents de la liste ou `Exited (0)` : **normal**, ce sont des tâches d'initialisation |

Si un service est `starting`, attends encore 1 minute et relance `docker compose ps`.

---

# LEVEL 1 — Ingestion dans le lakehouse

## Étape 6 — Créer les tables

```bash
make init
```

✅ **Contrôle** :
```bash
./scripts/trino.sh -e "SHOW SCHEMAS FROM iceberg"
```
La liste contient `audit`, `bronze`, `gold` et `silver`.

## Étape 7 — Générer les données

Les référentiels (50 000 clients, 80 000 comptes) puis les transactions du dernier
trimestre :
```bash
make generate-referentials CUSTOMERS=50000 ACCOUNTS=80000
make generate-events
```

✅ **Contrôle** : ouvre http://localhost:9001 (MinIO).
- Identifiant : `minio-admin`
- Mot de passe : `change-me-admin-password`

Le bucket `raw-landing` contient des dossiers `CI/`, `SN/`, `GH/`… et `referentials/`.

> L'application Streamlit (http://localhost:8501) permet aussi de générer des données à
> la souris.

## Étape 8 — Charger les fichiers dans Bronze

```bash
make ingest
make quality
```

✅ **Contrôle** : `make quality` affiche des tableaux, avec **0 clé orpheline** et
**0 doublon**. Les lignes rejetées sont celles injectées volontairement (1 %).

```bash
./scripts/trino.sh -e "SELECT country_code, count(*) AS nb FROM iceberg.bronze.bank_transactions GROUP BY 1 ORDER BY 1"
```
Résultat attendu : une ligne par pays.

🎉 **Level 1 terminé.**

---

# LEVEL 2 — Pipeline Airflow Bronze → Silver → Gold

## Étape 9 — Ouvrir Airflow

Ouvre http://localhost:8090.
- Identifiant : `admin`
- Mot de passe : `change-me-airflow-admin`

✅ **Contrôle** : 4 DAGs sont visibles : `dag_ingest_raw`, `dag_bronze_to_silver`,
`dag_silver_to_gold`, `dag_regulatory_report`.

## Étape 10 — Lancer la chaîne complète

Les fichiers de l'étape 7 ont déjà été chargés. On génère donc de nouvelles transactions,
puis on lance la chaîne :
```bash
make generate-events
make airflow-pipeline
```
La commande attend la fin des 3 DAGs (5 à 15 minutes). Pour suivre en direct, ouvre
Airflow → *Dags*.

✅ **Contrôle** : la commande se termine sans erreur et affiche `success` pour chaque DAG.
Dans Airflow, `dag_ingest_raw`, `dag_bronze_to_silver` et `dag_silver_to_gold` ont un
run vert.

## Étape 11 — Reporting réglementaire J+1

```bash
make airflow-regulatory
```
La date de reporting est choisie automatiquement **dans le trimestre généré**.

✅ **Contrôle** :
```bash
make regulatory-sql
```
Une déclaration BCEAO par pays, et des déclarations CIMA.

## Étape 12 — Voir les résultats

```bash
make medallion     # Bronze / Silver / Gold : volumes, unicité, EUR, pseudonymisation
make kpis          # les 7 indicateurs Gold
```

✅ **Contrôle** : les 7 tables Gold répondent. Le NPL est entre 3 et 8 % et le loss ratio
entre 50 et 85 % selon les pays.

🎉 **Level 2 terminé.**

---

# LEVEL 3 — Temps réel (NiFi → Kafka → Spark Streaming)

## Étape 13 — Démarrer le temps réel

```bash
make streaming-up
```
Attends **3 minutes**, NiFi est long à démarrer. Puis :
```bash
docker compose ps -a nifi-init kafka-init stream-silver stream-gold
```

✅ **Contrôle** :
- `kafka-init` et `nifi-init` affichent `Exited (0)` : c'est normal, ils ont fini leur travail.
- `stream-silver` et `stream-gold` sont `Up`.

Si `nifi-init` n'est pas encore `Exited (0)`, attends 2 minutes de plus. Sinon, lance :
```bash
docker compose logs nifi-init | tail -5
```
La dernière ligne doit contenir `"status": "CREATED"` (ou `"SKIPPED"`, si le flux
existait déjà).

## Étape 14 — Injecter des fraudes et un fichier défectueux

```bash
make fraud-demo
make dlq-demo
```
Attends **1 minute**.

✅ **Contrôle** :
```bash
make streaming-check
```

| Requête | Ce qu'on doit voir |
|---|---|
| 1 | des messages dans les 4 topics `raw-*`, avec `max_lag_s` sous 30 |
| 2 | `silver.rt_*` remplies |
| 3 | les règles `MULTIPLE_LARGE_TXN`, `UNUSUAL_COUNTRY`, `CLAIM_EXCEEDS_PREMIUM`, `AML_THRESHOLD_EXCEEDED` (XOF et GHS) et `LIQUIDITY_COVERAGE_BREACH` |
| 5 | des messages dans la file de rejets `dlq-financial-events` |

## Étape 15 — La requête Lambda (batch + temps réel)

```bash
make lambda
```

✅ **Contrôle** : la requête de l'énoncé s'exécute **sans erreur** et affiche une ligne
par pays et par jour.

## Étape 16 — Explorer

| Interface | Adresse | Connexion |
|---|---|---|
| NiFi | https://localhost:8443/nifi | `nifi-admin` / `change-me-nifi-password`. Le navigateur signale un certificat non sûr : clique sur *Avancé* → *Continuer* |
| Kafka UI | http://localhost:8084 | — |
| Trino (DBeaver) | `localhost`, port **8088**, base `iceberg` | utilisateur `admin`, sans mot de passe |

🎉 **Level 3 terminé.**

---

## Arrêter et relancer

```bash
docker compose --profile streaming stop    # arrête tout, garde les données
docker compose --profile streaming start   # redémarre
```
Pour tout effacer et recommencer : retourne à l'**étape 2**.

---

## Si quelque chose ne marche pas

| Message | Solution |
|---|---|
| `required variable PII_HASH_SECRET is missing` | Il manque le fichier `.env` dans ce dossier → étape 3 |
| `429 Too Many Requests` | `docker login`, puis les `docker pull` un par un → étape 4 |
| `pull access denied for minio/minio` | Tu utilises un ancien dossier : prends celui du dernier zip |
| `ReadTimeoutError` pendant `make up` | Connexion lente : relance `make up` |
| Airflow refuse la connexion | Identifiant `admin`, mot de passe `AIRFLOW_ADMIN_PASSWORD` dans `.env` |
| Une tâche Spark reste bloquée | Pas assez de mémoire pour Docker → étape 0 (12 Go minimum) |
| `make airflow-pipeline` attend sans fin | Ouvre Airflow et regarde la tâche rouge ; souvent, aucun fichier nouveau → refais l'étape 10 |
| `Table ... does not exist` | `make init`, puis refais l'étape concernée |
| `stream-silver` redémarre en boucle | Le Level 2 n'a pas encore tourné → étapes 10 et 11, puis il repart seul |
| `make streaming-check` : rien dans `raw-*` | `nifi-init` n'avait pas fini → attends, puis refais `make fraud-demo` |
| DBeaver : erreur `405` | Mauvais port : Trino est sur **8088** (8080, c'est Spark) |
| Erreur de mot de passe MinIO ou Airflow | Anciennes données Docker → étape 2, puis tout dans l'ordre |

Le Level 4 (Kubernetes, Superset, Grafana) se lance à part : voir la section
« Level 4 » du `README_COMPLET.md`.
