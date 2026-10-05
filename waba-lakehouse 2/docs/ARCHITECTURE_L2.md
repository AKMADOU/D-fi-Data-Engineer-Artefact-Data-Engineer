# Write-up technique — Level 2 : Orchestration & Architecture Médaillon

Le Level 2 industrialise le pipeline batch du Level 1 :
- **Apache Airflow 3.3** orchestre les jobs Spark ;
- les données sont structurées en trois zones Iceberg, **Bronze → Silver → Gold** ;
- une couche Gold produit les KPIs financiers et réglementaires des 8 pays ;
- un DAG quotidien génère le reporting BCEAO / CIMA à J+1.

```
 raw-landing (CSV) ──dag_ingest_raw──► bronze.*  ──asset──► dag_bronze_to_silver ──► silver.*
   (MinIO)            toutes les 15 min   (brut validé)                              (propre, EUR,
                      + détection fichiers                                            pseudonymisé)
                                                                                         │ asset
 regulatory-reports ◄── dag_regulatory_report (00h30 UTC, J+1) ◄── silver.*             ▼
   (CSV BCEAO/CIMA)      gold.regulatory_*                           gold.* ◄── dag_silver_to_gold
                                                                    (7 KPIs)
```

## 1. Orchestration Airflow

### Choix de déploiement

| Sujet | Choix | Justification |
|---|---|---|
| Version | Airflow **3.3.2** | Version maintenue. La branche 2.x est en fin de vie depuis avril 2026. Airflow 3 apporte le Task SDK, les Assets et l'API d'exécution |
| Exécuteur | `LocalExecutor` + PostgreSQL | Suffisant pour un poste unique, sans Celery/Redis. Le passage au `KubernetesExecutor` se fera au L4 |
| Composants | `api-server`, `scheduler`, `dag-processor` (+ `airflow-init`) | Architecture Airflow 3. Le triggerer est omis faute de tâches différées |
| Authentification | Simple auth manager, mot de passe admin fourni par `.env` | Aucun mot de passe généré aléatoirement ni écrit en dur |

### Lancement des jobs Spark : conteneur éphémère plutôt que `SparkSubmitOperator`

Chaque tâche Spark est un `SparkJobOperator`, une sous-classe de `DockerOperator`. Il crée
un conteneur à partir de l'image Spark du projet, connecté au réseau de la stack, qui
exécute `spark-submit`. Le driver tourne dans ce conteneur, les executors sur le worker
Spark.

- **Isolation des dépendances :** l'image Airflow n'embarque ni Java, ni PySpark, ni les
  JAR Iceberg. On évite aussi la contrainte « même version de Python entre driver et
  workers ».
- **Modèle identique à Kubernetes :** un conteneur par tâche, avec une image versionnée.
  Au L4, le passage à `KubernetesPodOperator` se fait sans toucher aux jobs.
- **Sécurité :** Airflow ne monte pas le socket Docker. Il passe par un
  `docker-socket-proxy` qui n'expose que les API conteneurs et images.
  - *Limite :* créer des conteneurs reste un privilège fort, acceptable en local. En
    production, ce rôle revient à Kubernetes (RBAC sur un namespace dédié).

### Les 4 DAGs

| DAG | Déclenchement | Tâches |
|---|---|---|
| `dag_ingest_raw` | Cron `*/15 * * * *` (variable `WABA_INGEST_SCHEDULE`) | `resolve_countries` → `detect_new_files` (short-circuit si rien de nouveau) → `ingest_referentials` → 4 × `ingest_<flux>` → `publish_bronze` (asset) |
| `dag_bronze_to_silver` | **Asset** `s3://lakehouse/bronze` | `resolve_countries` → `silver_referentials` → `silver_transactions` → `publish_silver` |
| `dag_silver_to_gold` | **Asset** `s3://lakehouse/silver` | `resolve_countries` → `gold_banking` ‖ `gold_insurance` ‖ `gold_mobile_money` → `publish_gold` |
| `dag_regulatory_report` | Cron `30 0 * * *` UTC (J+1) | `resolve_countries` → `build_regulatory_report` → `check_exports` |

**« Lancé après » :** les dépendances entre DAGs passent par des **Assets**
(data-aware scheduling) plutôt que par un `TriggerDagRunOperator`. Chaque couche publie un
asset quand elle a réussi, et c'est cette publication qui déclenche la couche suivante.
- Si l'ingestion n'a rien trouvé, le short-circuit coupe la chaîne : aucun recalcul inutile.
- Le lien entre couches est visible dans la vue *Assets* d'Airflow, ce qui prépare le
  lineage demandé au L4.

### Bonnes pratiques demandées

- **Dépendances explicites :** opérateur `>>` et TaskFlow API (`@task`, `@task.short_circuit`).
- **Retries :** 2 par défaut (3 pour le reporting), backoff exponentiel ×2 plafonné à
  10 min, et timeout d'exécution. Les retries sont sûrs parce que tous les jobs sont
  idempotents (voir § 3).
- **Alertes :**
  - `on_failure_callback` émet un log structuré `ALERT {...}`, puis envoie un POST JSON sur
    le webhook de la Variable `alert_webhook_url` (compatible Slack / Teams) si elle est
    définie ;
  - `on_retry_callback` trace chaque nouvelle tentative ;
  - un webhook injoignable ne masque jamais l'erreur d'origine.
- **Aucun credential en dur :**
  - la Connection `minio_s3` et les Variables (`pii_hash_secret`, `waba_countries`,
    `alert_webhook_url`…) sont fournies par les variables d'environnement
    `AIRFLOW_CONN_*` / `AIRFLOW_VAR_*`, alimentées par `.env` ;
  - les secrets sont injectés dans le conteneur Spark par `private_environment` au moment
    de l'exécution : ils n'apparaissent ni dans la commande, ni dans les templates rendus,
    ni dans l'UI. Un test le vérifie.
- **Paramétrage par pays :**
  - chaque DAG accepte un paramètre `countries`, validé contre la liste des 8 pays ;
  - priorité : conf du déclenchement > Variable `waba_countries` > 8 pays ;
  - les jobs ne recalculent que les partitions des pays demandés.
- **Pool `spark` :** un job Spark à la fois par défaut (`SPARK_POOL_SLOTS`), dimensionné
  pour un poste de 8 à 16 Go.

## 2. Architecture Médaillon

| Zone | Contenu | Partitionnement | Écriture |
|---|---|---|---|
| **Bronze** `bronze.*` (8 tables) | Lignes CSV ayant passé la validation de schéma, sans transformation métier, avec traçabilité (`_source_file`, `_batch_id`, `_ingested_at`) | `country_code`, `days(_ingested_at)` (date d'ingestion) | `MERGE` sur la clé métier (événements : insert-only ; référentiels : upsert) |
| **Silver** `silver.*` (8 tables + `fx_rates`) | Données dédupliquées, normalisées, enrichies par les référentiels, converties en EUR, pseudonymisées | Référentiels : `country_code` ; événements : `country_code`, `months(timestamp)` | Réécriture dynamique des partitions (pays, mois) |
| **Gold** `gold.*` (7 KPIs + 2 tables réglementaires) | Agrégats journaliers ou mensuels par pays et entité | `country_code` (réglementaire : `report_date`, `country_code`) | Réécriture dynamique des partitions par pays |
| **Audit** `audit.*` | Rejets de validation, journal d'ingestion, quarantaine Silver | — | Append ; la quarantaine est purgée puis réécrite |

> **Level 1 → Level 2 :** la zone `raw.*` du Level 1 correspond exactement à ce que
> l'énoncé appelle Bronze : même contrat, même validation, même `MERGE`. Le job
> d'ingestion écrit désormais dans `bronze.*`, partitionné par date d'ingestion comme
> demandé. L'option `--namespace raw` conserve le comportement du Level 1, et
> `make bootstrap-bronze` recharge l'historique archivé dans Bronze.

### Transformations Silver (`spark/jobs/common/silver_transforms.py`)

- **Déduplication sur la clé métier :** on garde la dernière version ingérée. Bronze est
  déjà unique grâce au `MERGE`, mais Silver ne suppose rien de sa source. Un contrôle
  d'unicité est exécuté après chaque écriture, et le job échoue en cas de doublon.
- **Normalisation et valeurs nulles :**
  - codes trimés et mis en majuscules ;
  - `channel`, `segment`, `region` et `operator` absents deviennent `UNKNOWN` ;
  - les frais absents deviennent 0 ;
  - `claim_status` et `processing_days` sont forcés à NULL hors sinistre.
- **Jointure avec les 4 référentiels :**
  - client → segment et KYC ;
  - compte → type et produit ;
  - agence → ville, région et type ;
  - produit → taux d'intérêt, qui sert au calcul des intérêts perçus.

  Les référentiels servent de table de correspondance sur **tous** les pays, même quand
  le run est limité à un pays : un virement international référence un compte étranger.
- **Conversion en EUR au taux du mois de l'opération** (table `silver.fx_rates`) :
  - XOF : parité fixe, 1 EUR = 655,957 XOF ;
  - GHS : série mensuelle de référence fictive, cohérente avec le générateur.
- **Pseudonymisation :**
  - `customer_id`, `account_id`, `sender_id`, `receiver_id`, `beneficiary_account` et
    `loan_account_id` sont remplacés par des `*_key` = SHA-256 d'un secret
    (`PII_HASH_SECRET`) concaténé à l'identifiant ;
  - la valeur étant déterministe, les jointures restent possibles entre Silver et Gold ;
  - elle est irréversible sans le secret ;
  - les identifiants réels ne subsistent qu'en Bronze, zone à accès restreint.
- **Intégrité référentielle :** une ligne dont une clé étrangère est absente (compte,
  bénéficiaire, agence, client, prêt non `LOAN`…) part dans `audit.silver_quarantine` avec
  son motif. Elle n'est pas perdue, et elle ne fausse pas les KPIs.
- **Cas aberrants :** montant supérieur à 1 M EUR, négatif ou nul, ou horodatage dans le
  futur. Les lignes sont **signalées** (`is_aberrant`) et exclues des KPIs, mais conservées
  pour investigation. `is_high_value` (≥ 10 M XOF) alimente le reporting BCEAO.

### KPIs Gold (`spark/jobs/common/gold_transforms.py`)

| Table | Grain | Formule / règle |
|---|---|---|
| `daily_transaction_volume` | jour × pays × entité × type | Volume, montant total et montant réussi en EUR (et en devise locale), taux d'échec. Couvre la banque et le mobile money |
| `npl_ratio_by_country` | pays × type de prêt (+ `ALL`) | Encours des prêts non performants / encours total. Un prêt est non performant si sa dernière échéance connue est en défaut ou en retard de plus de 90 jours. Seuil BCEAO de 5 % signalé |
| `customer_arpu_monthly` | mois × pays × segment | (commissions + intérêts perçus) / nombre de clients actifs distincts dans le mois |
| `loss_ratio_by_product` | mois × pays × produit | Sinistres payés / primes acquises (primes + renouvellements). Seuil CIMA de 70 % signalé |
| `claims_processing_time` | mois × pays × IARD/VIE | Délai moyen, médian et P90 de traitement en jours ouvrés, sinistres en attente |
| `mobile_money_daily_flow` | jour × pays | Volume, montants, taux d'échec, utilisateurs actifs (émetteurs distincts), frais, transfrontaliers |
| `cross_border_transfers` | semaine × corridor × canal | Transferts réussis via mobile money ou virement international. Nombre, montant total et moyen, évolution hebdomadaire (`lag`), indicateur de corridor UEMOA |

**Réalisme des données.** L'énoncé demande un NPL entre 3 % et 8 % et un loss ratio entre
50 % et 85 %. Le générateur est donc calibré par pays :
- un taux de défaut propre à chaque pays ;
- un loss ratio cible propre à chaque pays ;
- un **tirage à effectifs exacts** (stratifié) : la proportion de défauts et de sinistres
  est exacte même sur un petit échantillon ;
- des prêts tirés sans remise dans chaque lot.

Mesuré sur un jeu de 5 000 remboursements :
- NPL entre 3,4 % et 7,7 % selon les pays, certains au-dessus du seuil BCEAO et d'autres
  en dessous ;
- loss ratio trimestriel entre 58 % et 85 %.

Au grain mois × produit, le loss ratio reste naturellement plus dispersé : il ne porte que
sur quelques sinistres par cellule.

### Reporting réglementaire J+1 (`spark/jobs/regulatory_report.py`)

Pour une date de reporting D (la veille par défaut, ou `report_date` en paramètre pour
rejouer une date passée) :

- **`gold.regulatory_bceao_daily`**, par pays :
  - activité du jour : volumes, opérations de montant élevé, mobile money, sorties
    transfrontalières ;
  - stocks : dépôts, encours de crédit ;
  - NPL arrêté à D, avec indicateur de dépassement du seuil. L'arrêté exclut les
    échéances postérieures à D, ce qui rend le rejeu cohérent.
- **`gold.regulatory_cima_daily`**, par pays et produit : primes et sinistres cumulés du
  mois à date, loss ratio avec alerte, sinistres en attente et délai moyen.
- **Fichiers de déclaration CSV** dans
  `s3://regulatory-reports/<bceao|cima>/report_date=D/country_code=XX/`. Le bucket est
  versionné, et l'écrasement est dynamique : rejouer un pays ne supprime pas les autres.
- La tâche `check_exports` contrôle ensuite la complétude : un fichier BCEAO par pays.

## 3. Idempotence et reprise sur incident

Toutes les écritures sont rejouables. C'est ce qui rend les retries automatiques sûrs.

| Couche | Mécanisme |
|---|---|
| Bronze | Journal d'ingestion (clé + ETag, par table cible), `MERGE` sur la clé métier, archivage **après** le commit Iceberg |
| Silver / Gold | `overwritePartitions()` : réécriture atomique des seules partitions (pays [, mois]) recalculées |
| Quarantaine | `DELETE` des lignes des datasets et pays traités, puis append |
| Réglementaire | Partitions (`report_date`, pays) réécrites ; CSV en écrasement dynamique |

Les lectures se font sur un snapshot Iceberg. Un DAG Gold ou réglementaire qui lit Silver
pendant sa réécriture voit donc un état cohérent, soit l'ancien, soit le nouveau.

## 4. Tests

| Suite | Contenu |
|---|---|
| `generator/tests` (17) | Intégrité référentielle, devises, nommage, reproductibilité |
| `spark/tests/test_validation.py` (24) | Chaque règle de rejet Bronze, dédoublonnage, DDL/MERGE |
| `spark/tests/test_medallion.py` (13) | Mini-jeu Bronze construit à la main. Vérifie la déduplication, la quarantaine, la conversion EUR, la pseudonymisation (aucun identifiant en clair), NPL = 90 % sur un cas connu, loss ratio = 65 %, les corridors, l'ARPC, le reporting « as of » et la validité du DDL Silver/Gold |
| `airflow/tests` (8) | Import des 4 DAGs, retries et callbacks, dépendances, chaînage par assets, cron J+1 et calcul de D, secrets absents de la commande, webhook d'alerte |

La logique Spark et les DAGs ont été exécutés localement (PySpark 3.5.3, Airflow 3.3.2).
L'exécution complète dans Docker se vérifie avec `make demo-l2`.

## 5. Limites connues et évolutions

| Sujet | Limite | Évolution |
|---|---|---|
| Recalcul Silver/Gold | Recalcul complet par pays à chaque run : simple et idempotent, mais le coût croît avec l'historique | Incrémental par watermark sur `_ingested_at` ou Iceberg *incremental read* entre deux snapshots |
| Délai d'ingestion | Détection par sondage toutes les 15 min | Événements de bucket MinIO → Kafka au L3, ou `AssetWatcher` Airflow |
| Pool Spark à 1 slot | Les 3 domaines Gold s'exécutent l'un après l'autre sur un petit poste | `SPARK_POOL_SLOTS` et nombre de workers à augmenter selon les ressources |
| Taux GHS | Série fictive déterministe | Flux quotidien de cours de référence (Bank of Ghana) |
| NPL | Défini sur les prêts ayant au moins une échéance observée | Encours complet du portefeuille, avec l'historique des échéances depuis le core banking |
| Qualité | Contrôles codés dans les jobs (unicité, quarantaine) | Great Expectations / Soda, et métriques de qualité dans Grafana (L4) |
| Sécurité | Webhook d'alerte simple, conteneurs créés via proxy Docker | Alertmanager / PagerDuty ; `KubernetesPodOperator` avec RBAC (L4) |
