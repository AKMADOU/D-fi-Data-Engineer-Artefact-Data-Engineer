# Write-up technique — Lakehouse multi-pays WABA Group

*Choix d'architecture, compromis et limites connues (Levels 1 à 4).*

## 1. Le problème

WABA Group opère dans 8 pays d'Afrique de l'Ouest et 4 métiers : banque, assurance,
mobile money et microfinance. Le groupe manipule deux devises (XOF en UEMOA, GHS au
Ghana) et répond à deux régulateurs (BCEAO, CIMA). La plateforme doit :

- ingérer des fichiers hétérogènes et en garantir la qualité ;
- produire des indicateurs consolidés en EUR et des déclarations réglementaires à J+1 ;
- détecter en quasi-temps réel la fraude, le blanchiment et les tensions de liquidité ;
- exposer le tout avec un contrôle d'accès par pays et par rôle.

## 2. Principes directeurs

1. **Un seul format de table, un seul catalogue.** Toutes les couches sont des tables
   Apache Iceberg déclarées dans un catalogue REST, partagé par Spark (écriture) et Trino
   (lecture, SQL fédéré). Iceberg apporte :
   - les transactions ACID sur du stockage objet ;
   - le `MERGE` idempotent ;
   - le partitionnement caché (`country_code`, `days(timestamp)`) ;
   - l'évolution de schéma sans réécriture ;
   - le time-travel pour l'audit.
2. **Idempotence partout.** Rejouer un fichier, un job ou un micro-lot ne crée aucun
   doublon :
   - Bronze : `MERGE` sur la clé métier ;
   - Silver et Gold : réécriture des seules partitions (pays × mois) recalculées ;
   - streaming : `MERGE` sur la clé et `alert_id` déterministe ;
   - provisioning NiFi, OpenMetadata et Superset : relançables sans effet de bord.
3. **Même code en batch et en temps réel.** Le Job 1 streaming réutilise les règles de
   qualité du Level 1 et les transformations Silver du Level 2. Les deux couches de
   l'architecture Lambda ne peuvent pas diverger sur la validation, la conversion EUR ou
   la pseudonymisation.
4. **Aucun secret dans le code ni dans git.**
   - docker compose : variables `.env` ;
   - Airflow : Connections et Variables injectées par l'environnement ;
   - Kubernetes : Secrets générés hors dépôt, placeholders `${VAR}` dans le realm Keycloak.
5. **Minimisation des données personnelles.** Les identifiants clients et comptes
   n'existent en clair qu'en Bronze, zone réservée aux administrateurs. À partir de
   Silver, ils sont remplacés par un SHA-256 salé (`*_key`) : déterministe, donc les
   jointures restent possibles, mais irréversible sans le secret.

## 3. Choix par niveau

### Level 1 — Ingestion et lakehouse

- **Générateur Streamlit + CLI**, cohérent par construction : les clés étrangères existent,
  la devise suit le pays, chaque entité n'opère que dans ses pays. Des anomalies (doublons,
  devises incohérentes, lignes tronquées) peuvent être injectées pour éprouver la qualité.
- **Stockage MinIO** : l'image officielle ayant été retirée des registres publics
  (septembre 2026), nous utilisons le build communautaire `pgsty/silo`, même API S3.
  Un compte de service dispose d'une policy limitée aux buckets utiles ; le compte root ne
  sert qu'à l'initialisation.
- **Ingestion Spark** :
  - schéma explicite, mode PERMISSIVE (une ligne illisible est rejetée, jamais perdue) ;
  - motifs de rejet normalisés, tracés dans `audit.rejected_records` et dans le journal
    d'ingestion ;
  - archivage des fichiers traités.

### Level 2 — Médaillon et orchestration

- **Airflow 3** :
  - 4 DAGs chaînés par *Assets* : la publication de Bronze déclenche Silver, qui déclenche Gold ;
  - paramètre `countries` ;
  - retries avec backoff exponentiel ;
  - callbacks d'alerte (webhook) ;
  - pool pour ne pas saturer le cluster Spark.
- **Lancement des jobs Spark** :
  - docker compose : conteneurs éphémères via un proxy Docker restreint (Airflow ne monte
    jamais le socket Docker) ;
  - Kubernetes : SparkApplication soumise au Spark Operator. Le même DAG fonctionne dans
    les deux cas (`WABA_SPARK_BACKEND`).
- **Silver** : dédoublonnage, normalisation, enrichissement par les référentiels,
  conversion en EUR au taux du mois de l'opération. Les **orphelins** partent en
  quarantaine et les **aberrants** sont marqués, pas supprimés, pour garder l'audit.
- **Gold** : 7 KPIs recalculés par domaine métier (tables distinctes, donc tâches
  parallélisables sans conflit d'écriture) et 2 tables réglementaires J+1 exportées en CSV
  par pays pour la déclaration.

### Level 3 — Lambda

- **NiFi** : liste le bucket `raw-landing` et publie chaque ligne CSV en JSON dans le topic
  du flux. Il ajoute `ingestion_timestamp`, `source_file` et `landed_at`, et applique un
  back-pressure de 10 000 fichiers / 1 Go par connexion. NiFi ne valide rien : la
  validation est centralisée dans Spark.
- **Kafka 4.3 en KRaft** (plus de ZooKeeper) : topics créés explicitement, rétention
  adaptée à chaque couche (alertes conservées 90 jours).
- **Job 1** :
  - déduplication sur 10 min, fondée sur l'horodatage Kafka (monotone) plutôt que sur
    l'horodatage métier, pour ne pas écarter des données historiques rejouées ;
  - double sink : Iceberg d'abord, puis Kafka ;
  - DLQ qui transporte le message d'origine et le motif du rejet.
- **Job 2** :
  - fenêtres glissantes de 5 min avec un pas d'1 min, et un état qui traverse les micro-lots ;
  - seuils AML **en devise locale** (1 M XOF, 5 000 GHS) ;
  - réserve de liquidité = 3 % des dépôts ;
  - anti-rafale : une même rafale vue par plusieurs fenêtres ne produit qu'une alerte.
- **Stockage de la speed layer** : tables `silver.rt_*` séparées de Silver batch, pour ne
  pas entrer en conflit de commit avec les réécritures de partitions du batch. La batch
  layer recalcule la vérité depuis Bronze, et Trino combine les deux vues (requête Lambda
  exécutée telle qu'écrite dans l'énoncé).

### Level 4 — Production-grade

- **Kustomize plutôt qu'un chart Helm** : un rendu déterministe, vérifiable hors cluster
  (kubeconform strict + contrôle des références croisées), et des Secrets générés sans
  plugin.
- **Autorisation appliquée à la source** : Superset exécute les requêtes sous l'identité
  SSO de l'utilisateur, et Trino applique filtres de lignes par pays, masques de colonnes
  et restriction aux tables réglementaires. Un utilisateur qui passe par SQL Lab, DBeaver
  ou un notebook reçoit exactement les mêmes droits que dans les dashboards. Superset seul
  ne garantirait pas cela.
- **Une source unique pour les identités** : `k8s/users.json` génère à la fois le realm
  Keycloak, le fichier de groupes et les règles de Trino. Un test vérifie que les deux
  restent synchronisés.
- **Observabilité orientée métier** :
  - le driver streaming expose son lag Kafka (Spark ne publie pas ses offsets dans un
    consumer group) et sa latence par pays ;
  - Airflow publie l'horodatage de chaque succès et de chaque échec de DAG ;
  - les 3 alertes sont testées unitairement (`promtool test rules`).

## 4. Compromis assumés

| Compromis | Raison | Évolution en production |
|---|---|---|
| Catalogue Iceberg REST adossé à SQLite | Simplicité, un seul écrivain | Catalogue REST sur PostgreSQL, ou Nessie / Polaris (branches de données) |
| Spark local dans les pods streaming | Sondes propres au driver, isolation des ressources batch | SparkApplication en mode cluster avec plusieurs executors et un checkpoint S3 |
| Airflow en LocalExecutor | Minikube, faible volumétrie | KubernetesExecutor, DAGs synchronisés par git-sync |
| Mono-nœud (Kafka, Trino, PostgreSQL) | Tient sur un poste de 24 Go | 3 brokers (RF=3), des workers Trino, PostgreSQL managé |
| Certificat TLS auto-signé | Démonstration locale | cert-manager + ACME ou PKI d'entreprise |
| Taux GHS/EUR mensuels simulés | Pas d'accès à une source officielle | Référentiel de change alimenté par la BCEAO ou la Bank of Ghana |
| Débit NiFi mesuré via les offsets Kafka | L'endpoint Prometheus de NiFi exige un jeton à durée limitée | Reporting task ou exporter avec un compte de service NiFi |

## 5. Limites connues

- **Tests de bout en bout en conditions réelles** : les Levels 1 et 2 ont tourné sur un
  poste utilisateur. Les Levels 3 et 4 ont été validés :
  - par des tests unitaires : Spark (dont de vrais tests Structured Streaming), Airflow,
    générateur, NiFi et OpenMetadata contre de faux serveurs ;
  - par des exécutions réelles de composants isolés : import Superset, realm Keycloak,
    Prometheus, Loki et Promtail ;
  - pas par un déploiement complet sur cluster, faute de Docker et de Kubernetes dans
    l'environnement de développement.
- **Données synthétiques** : les volumes et les distributions sont calibrés pour des
  indicateurs réalistes (NPL entre 3 et 8 %, loss ratio entre 50 et 85 %), mais ce ne
  sont pas des comportements réels.
- **Détection de fraude à base de règles** : les seuils sont fixes. Un modèle de scoring
  (isolation forest, graphes de transactions) serait l'étape suivante, alimentée par les
  mêmes topics Silver.
- **Rétention et droit à l'oubli** : la pseudonymisation couvre Silver et Gold, mais
  l'effacement d'un client en Bronze (RGPD, article 17) n'est pas outillé. Iceberg le
  permettrait (`DELETE` suivi d'une expiration des snapshots).
