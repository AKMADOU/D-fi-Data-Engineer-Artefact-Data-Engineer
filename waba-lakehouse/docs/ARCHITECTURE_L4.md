# Level 4 — Production-grade : Kubernetes, gouvernance et observabilité

## 1. Vue d'ensemble

```
                             Ingress nginx (TLS *.waba.local)
   ┌──────────────┬──────────────┬───────────────┬────────────────┬───────────────┐
   │ ingestion    │ processing   │ serving       │ governance     │ monitoring    │
   │──────────────│──────────────│───────────────│────────────────│───────────────│
   │ MinIO (PVC)  │ Spark Operator│ Trino (TLS,  │ Keycloak (SSO) │ Prometheus    │
   │ Kafka KRaft  │  + CRD        │  OAuth2+pwd, │ OpenMetadata   │ Pushgateway   │
   │  (PVC)       │ Airflow 3     │  règles)     │ Elasticsearch  │ statsd-export.│
   │ NiFi (PVC)   │  (Postgres PVC)│ Superset    │ PostgreSQL PVC │ kafka-exporter│
   │ Streamlit    │ Iceberg REST  │  (Postgres)  │ CronJob de     │ Loki + Promtail│
   │ Kafka UI     │ stream-silver │               │  gouvernance   │ Grafana       │
   │              │ stream-gold   │               │                │ alert-webhook │
   └──────────────┴──────────────┴───────────────┴────────────────┴───────────────┘
```

**Une seule commande** : `kubectl apply --server-side -k .` (ou `make k8s-up`, qui prépare
aussi minikube, les images et les secrets). Le `kustomization.yaml` racine assemble les
5 namespaces et génère ConfigMaps et Secrets. `--server-side` est nécessaire parce que les
CRD du Spark Operator dépassent la taille d'annotation du mode client.

### Pourquoi Kustomize et pas un chart Helm

L'énoncé accepte `helm install` ou `kubectl apply -f`. Nous avons choisi Kustomize
(`kubectl apply -k`, intégré à kubectl) pour trois raisons :

1. **Le rendu est vérifiable hors cluster.** `kustomize build` produit exactement les
   135 ressources appliquées. Elles sont validées en mode strict contre les schémas
   Kubernetes 1.34 (kubeconform) et par un contrôle de références croisées
   (`scripts/k8s/check_manifests.py`) : chaque Secret, ConfigMap, clé, PVC,
   ServiceAccount, Service et backend d'Ingress référencé existe.
2. **Les Secrets sont générés sans être versionnés.** `secretGenerator` lit
   `k8s/.generated/*.env`, produit par `make k8s-secrets` et ignoré par git. Les
   manifestes ne contiennent aucun secret, sans dépendre d'un plugin Helm
   (helm-secrets, SOPS).
3. **Aucune dépendance réseau au déploiement.** Le Spark Operator est vendorisé : les CRD
   et le contrôleur sont rendus depuis le chart officiel 2.5.2, avec le webhook désactivé.

## 2. Exigences 4.1 — déploiement Kubernetes

| Exigence | Réalisation |
|---|---|
| MinIO, Spark (Spark Operator), Airflow, Kafka, NiFi, Trino sur K8s | StatefulSets : MinIO, Kafka, NiFi, les PostgreSQL, Elasticsearch, Loki. Deployments : le reste. Les jobs Spark batch sont des **SparkApplication** soumises par Airflow au Spark Operator (`SparkK8sJobOperator`) |
| Namespaces par domaine | `ingestion`, `processing`, `serving`, `governance`, `monitoring` (`k8s/namespaces.yaml`) |
| ConfigMaps et Secrets, aucun secret en clair | Configuration en ConfigMaps. Secrets générés depuis `.env` + `k8s/secrets.env` (valeurs aléatoires au premier lancement). Le contrôle `check_manifests.py` refuse toute variable `*PASSWORD/SECRET/KEY/TOKEN` écrite en dur. Le realm Keycloak contient des placeholders `${VAR}`, remplacés au démarrage. Les SparkApplication reçoivent leurs identifiants par `envFrom: secretRef` dans le pod template, jamais dans la ressource |
| Liveness & readiness probes | **Kafka** : readiness = API broker (`kafka-broker-api-versions`), liveness = port. **Driver Spark** : serveur HTTP du driver (`common/metrics.py`) ; `/ready` = toutes les requêtes Structured Streaming actives et ayant progressé depuis moins de 2 min ; `/healthz` = aucune requête arrêtée en erreur et aucun blocage de plus de 10 min. **Coordinateur Trino** : `health-check` de l'image et `/v1/info`. Également sondés : NiFi, MinIO, Keycloak (`/health/*`), OpenMetadata, Airflow, Superset, les PostgreSQL, Grafana, Loki, Prometheus |
| PVC | MinIO, bases Airflow / Superset / gouvernance (Keycloak + OpenMetadata), Elasticsearch d'OpenMetadata, Kafka, NiFi, checkpoints des 2 jobs streaming, catalogue Iceberg, Prometheus, Loki, Grafana |
| Services et Ingress | Airflow, Superset, NiFi, Grafana, OpenMetadata, plus Keycloak, Trino, MinIO et Streamlit, en TLS (certificat auto-signé `*.waba.local`). Kafka UI, sans authentification, reste accessible par `kubectl port-forward` uniquement |

**Choix pour les jobs streaming.** Les deux drivers Structured Streaming du Level 3
tournent comme **Deployments longue durée** (Spark en mode local dans le pod, checkpoints
sur PVC). Ils ne passent pas par une SparkApplication, pour trois raisons : garder des
sondes liveness/readiness propres au driver, obtenir un redémarrage automatique depuis le
checkpoint, et ne pas consommer les ressources des jobs batch. Les jobs batch (ingest,
silver, gold, reporting) passent par le Spark Operator, avec un driver et un executor par
job.

## 3. Superset (4.2)

- L'image `waba/superset:6.1.0-waba1` ajoute le pilote Trino et Authlib à l'image officielle.
- Le Job `superset-init` enchaîne les migrations, le compte local de secours et un **import
  idempotent** des 3 dashboards. Ceux-ci sont décrits en Python (`superset/bootstrap/waba_bundle.py`,
  format d'export v1) : 10 datasets virtuels SQL sur Silver et Gold, et 13 graphiques.
- **Validation** : l'import a été exécuté sur un Superset 6.1.0 réel (métadonnées SQLite
  locales). Il crée 3 dashboards, 13 graphiques et 10 datasets. Le filtre natif « Pays »
  est rattaché à un dataset, les identifiants de graphiques sont remappés, et un
  réimport donne un résultat identique. Le rendu des graphiques avec les données réelles
  de Trino n'a pas pu être testé hors cluster.
- **Filtre par pays** : chaque dashboard a un filtre natif « Pays » sur `country_code`. Tous
  les datasets exposent cette colonne (vérifié par `superset/tests/test_bundle.py`).

| Dashboard | Graphiques |
|---|---|
| Performance commerciale | Revenus par pays × ligne métier (barres groupées) · ARPC mensuel par pays · carte choroplèthe (`world_map`, codes ISO-2) de la contribution des pays aux revenus · Top 10 des produits souscrits par pays |
| Risque & conformité | Taux NPL par pays (vert < 3 %, orange 3-5 %, rouge > 5 %) + barres · loss ratio par produit et pays (seuil CIMA 70 %, mise en forme rouge au-delà) · alertes AML par jour et par pays sur 30 jours (table `gold.aml_events` du Level 3) · délai moyen de traitement des sinistres vs SLA de 30 jours |
| Mobile Money & transferts | Heatmap pays × heure · top 5 des corridors transfrontaliers · taux d'échec par opérateur et pays |

## 4. Gouvernance et sécurité (4.3)

### Keycloak (SSO OpenID Connect)

- Le realm `waba` est **généré** depuis `k8s/users.json` par `scripts/k8s/render_identity.py`.
  Il contient 4 rôles métier (`group_admin`, `country_analyst`, `compliance_officer`,
  `viewer`), 8 rôles de périmètre `country_XX`, les clients OIDC `superset` (PKCE S256)
  et `trino`, un mapper `groups` (rôles du realm) et un mapper d'audience.
- **Validation** : le realm a été importé sur un Keycloak 26.7.5 réel. Vérifié : les
  placeholders de secrets sont remplacés, le jeton d'`analyste.ci` porte
  `groups=[country_CI, country_analyst]` et `aud=trino`, et l'émetteur reste
  `https://keycloak.waba.local/...` même quand le jeton est demandé par l'URL interne
  (`KC_HOSTNAME` + `KC_HOSTNAME_BACKCHANNEL_DYNAMIC`).
- **Superset** : `AUTH_OAUTH`, rôles synchronisés à chaque connexion (`group_admin → Admin`,
  `country_analyst → WABA_Analyst`, `compliance_officer → WABA_Compliance`,
  `viewer → WABA_Viewer`). Le navigateur passe par l'URL publique de Keycloak, l'échange
  du code se fait en interne.
- **Trino** : `OAUTH2` pour les humains (interface web et JDBC `externalAuthentication`),
  `PASSWORD` (fichier PBKDF2) pour les comptes de service `superset`, `prometheus` et
  `openmetadata`. Trino est en HTTPS (OAuth2 exige TLS et un secret partagé interne).

### Autorisation : appliquée dans Trino

Superset exécute les requêtes **sous l'identité de l'utilisateur SSO** (impersonation
autorisée pour le seul compte `superset`). Les règles de Trino
(`k8s/serving/trino/rules.json`, générées) s'appliquent donc aussi dans les dashboards :

| Rôle | Accès |
|---|---|
| `group_admin` | Tout |
| `country_analyst` + `country_CI` | Silver et Gold, **filtre de lignes** `country_code = 'CI'` ; pas de Bronze |
| `compliance_officer` | Tables Gold réglementaires uniquement (`regulatory_*`, `aml_events`, `fraud_alerts`, `liquidity_alerts`, NPL, loss ratio, sinistres) |
| `viewer` | KPIs Gold et Silver avec **masques de colonnes** : identifiants pseudonymisés remplacés par `***`, montants de sinistres individuels à NULL. Refus sur Bronze (seule couche contenant les identifiants de comptes en clair) et sur les alertes |

Les données ne contiennent ni IBAN ni numéro de compte au sens bancaire. Les
équivalents sont `account_id` et `beneficiary_account`, traités comme tels.

### OpenMetadata (catalogue)

- Le serveur 1.13.6 s'appuie sur Elasticsearch et PostgreSQL. Le CronJob
  `openmetadata-governance` s'exécute toutes les heures, ou à la demande avec
  `make k8s-governance`. Il enchaîne :
  1. ingestion **Trino** (bronze, silver, gold) puis **Kafka** (topics raw, silver, gold, DLQ) avec la CLI `metadata` ;
  2. `governance/openmetadata_bootstrap.py`, qui applique :
     - la classification `Reglementaire` (BCEAO, CIMA, AML, RGPD) ;
     - 5 équipes propriétaires (entités WABA) ;
     - un glossaire financier de 8 termes ;
     - la documentation de 12 tables Gold (description métier, propriétaire, tags réglementaires, termes du glossaire, Tier 1) ;
     - `PII.Sensitive` + `Reglementaire.RGPD` sur `customer_id`, `account_id`, `beneficiary_account`, `sender_id`, `receiver_id`, `loan_account_id` (Bronze) et sur leurs versions pseudonymisées `*_key` (Silver et Gold) ;
     - le **lineage raw → Bronze → Silver → Gold** : la couche raw est représentée par les conteneurs S3 du bucket `raw-landing` ; la branche temps réel ajoute les conteneurs, les topics `raw-*` / `silver-*`, les tables `silver.rt_*` et les alertes Gold.
- Validation : schémas de l'API vérifiés sur la version 1.13.6 ; script testé contre un
  faux serveur OpenMetadata (ce test a révélé et fait corriger un vrai bug).

## 5. Observabilité (4.4)

| Exigence | Réalisation |
|---|---|
| Métriques Spark, Kafka, NiFi, Airflow, Trino | **Annotations** `prometheus.io/*` (drivers Spark streaming, Spark Operator, MinIO, Keycloak, OpenMetadata, Loki, Grafana). **Exporters** : `statsd-exporter` (Airflow StatsD, avec `dag_id`, `task_id`, état), `kafka-exporter` (offsets, consumer groups), Pushgateway (état des DAGs), Trino `/metrics` (compte `prometheus`). **NiFi** : son endpoint Prometheus exige un jeton d'accès à durée limitée ; son débit est donc mesuré par les offsets des topics `raw-*` qu'il alimente |
| Logs centralisés, parsing JSON des jobs Spark | Promtail (DaemonSet) → Loki. Les pods annotés `waba/log-format=json` (drivers batch et streaming) sont analysés : niveau, logger et statut deviennent des labels ; les champs métier (`duration_s`, `lag_by_country`...) restent requêtables par `| json`. Pipeline testé avec `promtail --dry-run --inspect` et requêtes testées avec `logcli` |
| Dashboard de santé | Grafana « WABA — Santé des pipelines » : latence ingestion → Silver **par pays** (variable Pays), durée des jobs batch (Loki), **lag des consumers Kafka de la détection de fraude**, **débit NiFi → Kafka**, **taux d'erreur des tâches et DAGs Airflow**, âge du dernier reporting réglementaire, débit Structured Streaming, alertes métier et DLQ, cibles indisponibles, erreurs applicatives |

Le « lag des consumers Kafka » : Spark Structured Streaming ne publie pas ses offsets dans
un consumer group Kafka (il les garde dans son checkpoint). Le driver calcule donc le lag
lui-même, dans un `StreamingQueryListener`, comme dernier offset du topic − offset
traité. C'est exactement ce que mesurerait un consumer group.

### Alertes

Les 3 alertes Grafana sont provisionnées (`grafana/provisioning/alerting.yaml`) et envoyées
au récepteur `alert-webhook`, qui les journalise, donc aussi dans Loki. Leurs expressions
sont **testées unitairement** avec `promtool test rules` (`k8s/monitoring/tests/`),
déclenchement et non-déclenchement :

| Alerte | Condition | Démonstration |
|---|---|---|
| Job Spark fraude en erreur > 5 min | Pod `stream-gold` absent, ou aucune progression Structured Streaming depuis 2 min, pendant 5 min | `make k8s-alert-demo A=fraud-down` |
| Consumer Kafka AML, lag > 5 000 | Σ lag de la requête `event_rules` (règles AML) > 5 000 pendant 1 min | `make k8s-alert-demo A=lag` (débit du consommateur limité puis 20 000 paiements injectés) |
| `dag_regulatory_report` en échec à J+1 06h00 UTC | Après 06h00 UTC, pas de succès depuis minuit ; ou dernier échec plus récent que le dernier succès (callbacks Airflow → Pushgateway) | `make k8s-alert-demo A=regulatory` |

`make k8s-alert-demo A=restore` remet la situation normale.

## 6. Limites connues

- **Aucun test en cluster réel depuis l'environnement de développement** (pas de Docker ni
  de Kubernetes). Ce qui a été validé : rendu Kustomize + kubeconform strict + références
  croisées, import Superset réel, realm sur Keycloak réel, configurations Prometheus, Loki
  et Promtail avec les binaires officiels, tests promtool des alertes, tests unitaires du
  code (Airflow, Spark, OpenMetadata et NiFi contre de faux serveurs).
- **Mono-nœud** : 1 broker Kafka, 1 coordinateur Trino, PostgreSQL sans réplication.
  Suffisant pour Minikube, pas pour la haute disponibilité.
- **Ressources** : profil `full` (~18 Go réservés, pour Minikube à 24 Go) ou `light`
  (~11,5 Go, pour un poste dont Docker dispose de 16 Go). Le profil est choisi
  automatiquement par `scripts/k8s/up.sh` et appliqué par un Component Kustomize généré
  (`k8s/profiles/light/patches.yaml`). En profil léger, les jobs Spark batch (driver
  768 Mo + executor 1 Go) doivent s'exécuter un par un (pool Airflow `spark` à 1 slot).
- **TLS auto-signé** : en production, cert-manager et une vraie autorité de certification.
- **Promtail** : c'est le collecteur demandé par l'énoncé, mais Grafana le remplace
  désormais par Alloy ; la configuration de collecte se transpose telle quelle.
- **Mots de passe par défaut d'OpenMetadata** (`admin@open-metadata.org` / `admin`) : à
  changer au premier login, ou brancher OpenMetadata sur Keycloak, ce que l'énoncé ne
  demande que pour Superset et Trino.
