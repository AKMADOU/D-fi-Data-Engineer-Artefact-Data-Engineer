# Write-up technique — Level 1 : Ingestion & Data Lakehouse batch

## 1. Vue d'ensemble

Le Level 1 pose le socle du pattern Lambda demandé : un stockage objet central (MinIO) au
format ouvert Apache Iceberg, alimenté par Spark et interrogé par Trino. Les niveaux suivants
(Bronze/Silver/Gold, Airflow, Kafka, Kubernetes) viennent se brancher sur ces briques sans
les remplacer.

| Couche | Choix | Pourquoi |
|---|---|---|
| Génération | Streamlit + package Python `waba_gen` (numpy/pandas vectorisé) | UI demandée ; la même logique est exposée en CLI, réutilisable par Airflow au L2 |
| Stockage | MinIO (build communautaire `pgsty/silo`), 3 buckets `raw-landing` / `lakehouse` / `archive` | Compatible S3, identique à une cible cloud ; les images officielles MinIO ne sont plus publiées depuis sept. 2026 |
| Format de table | Apache Iceberg v2 | Transactions ACID, `MERGE`, partitionnement caché, time travel, lu nativement par Spark et Trino |
| Catalogue | Iceberg **REST** (`tabulario/iceberg-rest`, SQLite persistant) | Plus léger qu'un Hive Metastore (pas de Postgres + HMS), standard ouvert, un seul catalogue partagé par Spark et Trino |
| Traitement | Spark 3.5.3 standalone (1 master, 1 worker), PySpark | Exigé par l'énoncé ; mode cluster réaliste, sans surcoût |
| Exposition | Trino 460, connecteur Iceberg + S3 natif | SQL ANSI, requêtes fédérées, base de Superset au L4 |

## 2. Génération des données

**Cohérence référentielle par construction.** Les référentiels sont générés en premier et
conservés par le générateur (parquet dans un volume). Les transactions ne tirent leurs clés
que dans ces référentiels, et les jobs n'utilisent que des comptes `ACTIVE`. Il n'existe donc
aucun chemin de code qui produise une clé orpheline. Les règles métier du tableau
« Contexte & Mission » sont respectées :

- le Mobile Money n'existe qu'en CI, SN, BF et GH, la Microfinance qu'en ML, GN et BF ;
- l'Assurance Vie n'opère qu'en zone UEMOA, donc aucune opération `VIE` ou `PREVOYANCE` au Ghana ;
- le type de compte découle de l'entité (`MOBILE_WALLET` ↔ Mobile Money, `INSURANCE_POLICY` ↔ Assurance, `LOAN` ↔ Banque ou Microfinance) ;
- `loan_repayments.loan_account_id` est toujours un compte de type `LOAN` du même client ;
- la devise suit toujours le pays : XOF en UEMOA, GHS au Ghana. Les montants en GHS sont
  mis à l'échelle (≈ 1 GHS pour 50 XOF) pour rester réalistes.

**Nomenclature et partitionnement des fichiers.** Il y a un fichier par (pays, jour), sous la
forme `raw-landing/<CC>/<dataset>/<prefixe>_<CC>_<YYYYMMDD>_<NN>.csv`. `NN` est calculé à
partir des fichiers déjà présents dans `raw-landing` **et** `archive`, de sorte qu'une nouvelle
génération n'écrase jamais un fichier existant.

**Modes.** Le mode *one-time* couvre une période complète. Le mode *continu* produit un
micro-lot horodaté sur la fenêtre `[now − intervalle, now]`, avec un intervalle tiré entre
10 et 60 s. Pour les référentiels, le mode continu simule l'onboarding : les nouveaux clients
et comptes sont écrits dans des fichiers `*_delta_*.csv` et leurs identifiants continuent la
séquence existante.

**Reproductibilité.** Une graine fixe (défaut `42`) régénère exactement les mêmes référentiels.

**Anomalies contrôlées.** Un taux paramétrable injecte des devises incohérentes, des montants
négatifs, des identifiants manquants, des horodatages invalides et des doublons, afin de
démontrer la chaîne de validation.

**Écarts assumés au schéma de l'annexe.**
- `accounts` reçoit `entity_type` (contrainte « toutes les tables portent `entity_type` ») et
  `product_id`, nécessaire à la jointure `products` en couche Silver (L2).
- Le schéma de `products` n'est pas fourni : j'ai défini `product_id`, `product_name`,
  `product_category`, `entity_type`, `country_code`, `currency`, `interest_rate`, `monthly_fee`,
  `launch_date` et `is_active`, avec au moins un produit par (pays, entité, type de compte).
- `mobile_money_payments` n'a pas de `country_code` dans le CSV. La table Iceberg l'ajoute
  avec `country_code = sender_country`.

## 3. Ingestion Spark → Iceberg

`spark/jobs/ingest.py` traite chaque dataset en 7 étapes :

1. **Inventaire** de `raw-landing` (filtrable par pays) via l'API S3.
2. **Journal d'ingestion** (`audit.ingestion_log`) : un fichier déjà ingéré avec succès
   (même clé et même ETag) est ignoré.
3. **Contrôle de contrat** : l'en-tête CSV doit correspondre exactement au schéma. La lecture
   Spark étant positionnelle, un fichier aux colonnes permutées serait sinon chargé en silence
   dans les mauvaises colonnes. Un fichier non conforme est mis en quarantaine
   (`archive/_rejected_files/`).
4. **Lecture avec schéma explicite** en mode `PERMISSIVE` : une ligne illisible (horodatage
   invalide, montant non numérique, nombre de colonnes erroné) n'est pas perdue, elle arrive
   dans `_corrupt_record`.
5. **Validation** par expressions Spark natives, sans UDF Python : champs obligatoires,
   énumérations, pays, devise (XOF/GHS) **et cohérence devise ↔ pays**, montants ≥ 0, format
   de clé (UUID v4 ou `WABA-CC-X-n`). Chaque rejet porte la liste de ses motifs
   (`CURRENCY_COUNTRY_MISMATCH;NEGATIVE_AMOUNT`) et est conservé dans `audit.rejected_records`
   avec la ligne brute.
6. **Déduplication intra-lot** puis **`MERGE INTO`** sur la clé métier, en un seul commit
   Iceberg atomique :
   - pour les événements, `WHEN NOT MATCHED THEN INSERT` : rejouer un fichier n'ajoute rien ;
   - pour les référentiels, un upsert conditionnel qui ne met à jour que si un attribut a
     changé ; le fichier le plus récent gagne (delta > snapshot).
7. **Archivage** `raw-landing → archive` (copie puis suppression), **après** le commit. Si
   l'archivage échoue, le fichier sera relu au prochain run et le `MERGE` empêchera tout
   doublon : le traitement est rejouable à tout moment.

L'idempotence repose donc sur deux niveaux : le journal (optimisation, pas de relecture) et
le `MERGE` (garantie). `make idempotency` rejoue toute l'archive avec `--force` pour prouver
que le second niveau suffit.

**Modélisation des tables.**
- Partitionnement caché `(country_code, days(timestamp))` pour les événements et
  `(country_code)` pour les référentiels. Partitionner un référentiel par date n'apporte
  rien : on le lit toujours en entier ou par pays. Grâce au partitionnement caché, un filtre
  `WHERE timestamp >= …` bénéficie du pruning sans colonne `event_date` à maintenir.
- Montants en `DECIMAL(20,2)` plutôt que `FLOAT` : les sommes financières doivent être
  exactes (réconciliation, reporting réglementaire).
- Horodatages en UTC (`timestamptz`), Parquet compressé en ZSTD, Iceberg format v2 en
  *merge-on-read* : un `MERGE` ne réécrit pas les fichiers existants.
- Trois colonnes techniques par table (`_source_file`, `_batch_id`, `_ingested_at`) assurent
  la traçabilité ligne → fichier → lot. Elles serviront de base au lineage d'OpenMetadata (L4).

**Source unique de vérité.** `common/schemas.py` décrit chaque dataset (types, clé, règles,
partitionnement). Le DDL, la lecture CSV, la validation et le `MERGE` en sont tous dérivés,
et un test vérifie que le SQL généré est syntaxiquement valide.

## 4. Sécurité et exploitation

- **Aucun secret dans le code.** Tout vient de `.env`, injecté par Compose. Le catalogue Trino
  lit ses identifiants via `${ENV:…}`, et Spark masque les clés dans son UI.
- **Moindre privilège.** Le compte root MinIO ne sert qu'à l'initialisation. Les applications
  utilisent un compte de service dont la policy est limitée aux 3 buckets.
- **Pas d'opération destructive non contrôlée.** Un fichier n'est supprimé de `raw-landing`
  qu'après sa copie dans `archive`, qui est versionné. Les tables Iceberg conservent leur
  historique (time travel).
- **Données personnelles.** Le générateur ne produit ni nom, ni IBAN, ni téléphone : seulement
  des identifiants synthétiques. Le masquage et la pseudonymisation des identifiants de compte
  seront appliqués en couche Silver (L2), puis tagués PII dans OpenMetadata (L4).
- **Observabilité.** Les logs sont en JSON structuré (générateur et Spark), avec un résumé
  par dataset (lues / valides / rejetées / insérées / durée). Le journal d'ingestion est
  requêtable dans Trino.
- **Qualité.** Des tests unitaires couvrent le générateur (intégrité, devises, nommage, non
  écrasement, reproductibilité) et la validation Spark (chaque règle de rejet, dédoublonnage,
  DDL/MERGE). `sql/03_quality_checks.sql` fournit les contrôles de bout en bout (orphelins,
  doublons, devises).

## 5. Compromis et limites connues

| Sujet | Choix actuel | Évolution |
|---|---|---|
| Catalogue REST | Image de référence avec SQLite (mono-instance) | Postgres (catalogue JDBC) ou Nessie/Polaris en production |
| `MERGE` événements | Jointure sur la clé seule : coût proportionnel à la table | Ajouter `t.country_code = s.country_code AND date(t.timestamp) = …` pour un pruning par partition quand les volumes grandissent |
| Petits fichiers | Un fichier par (pays, jour, lot) en landing ; les commits fréquents du mode continu créent de petits fichiers Parquet | Compaction planifiée (`rewrite_data_files`, `expire_snapshots`) orchestrée par Airflow au L2 |
| Orchestration | Lancement manuel (`make ingest`) | DAG Airflow paramétré par pays au L2 |
| Contrôle référentiel | Garanti à la génération et vérifié en SQL après coup, pas bloquant à l'ingestion raw | Rejet ou quarantaine des orphelins en Silver (la couche raw reste fidèle à la source) |
| Delta référentiels | Nouveaux clients/comptes uniquement | Mises à jour de soldes ou de statuts (SCD2 en Silver) |
| Spark | 1 worker de 2 Go | Ajouter des workers (services supplémentaires) ou passer sur Kubernetes (L3-L4) |
| Sécurité Trino | Pas d'authentification | Keycloak/OAuth2 et contrôle d'accès par rôle au L4 |
| Générateur | Référentiels gardés en mémoire (~300 Mo pour 800 k comptes) | Suffisant pour le challenge ; lecture depuis Iceberg au-delà |
