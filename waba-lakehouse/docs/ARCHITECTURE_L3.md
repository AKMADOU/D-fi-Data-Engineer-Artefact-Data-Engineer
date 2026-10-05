# Level 3 — Pipeline hybride batch & streaming (architecture Lambda)

## 1. Vue d'ensemble

```
                         ┌──────────────── batch layer (L1/L2, Airflow, toutes les 15 min) ──┐
 Streamlit ─► MinIO      │ raw-landing ─► bronze.* ─► silver.* ─► gold.* (KPIs, BCEAO/CIMA)  │
            raw-landing ─┤                                                                   │
                         └──────────────── speed layer (L3, quelques secondes) ─────────────┐
   NiFi : ListS3 ─► Route ─► FetchS3Object ─► UpdateAttribute ─► UpdateRecord ─► PublishKafka│
                                                     (topic)       (CSV→JSON +        │      │
                                                                    métadonnées)       ▼      │
   Kafka : raw-bank-transactions · raw-insurance-operations · raw-mobile-money-payments ·    │
           raw-loan-repayments                                                              │
                 │ Spark Job 1 (stream_raw_to_silver)                                        │
                 ├─► silver-bank-transactions · silver-insurance-operations ·               │
                 │   silver-mobile-money · silver-loan-repayments   (Kafka)                 │
                 ├─► silver.rt_<dataset>                             (Iceberg, MERGE)        │
                 └─► dlq-financial-events                           (rejets + orphelins)     │
                 │ Spark Job 2 (stream_silver_to_gold)                                       │
                 └─► gold-fraud-alerts · gold-aml-events · gold-liquidity-alerts (Kafka)     │
                     gold.fraud_alerts · gold.aml_events · gold.liquidity_alerts (Iceberg)   │
                                                                                             │
 serving layer : Trino — catalogue iceberg (batch) + catalogue kafka (temps réel) ◄──────────┘
```

Batch et streaming lisent **la même source** (`raw-landing`) et appliquent **le même code** :
la validation du Level 1 (`validation.prepare_batch`) et les transformations Silver du
Level 2 (`silver_transforms.silver_*`) sont appelées telles quelles par le Job 1. Les deux
couches ne peuvent donc pas diverger sur les règles de qualité, la conversion EUR ou la
pseudonymisation.

## 2. NiFi (3.1)

| Exigence | Réalisation |
|---|---|
| Surveiller `raw-landing` (ListS3 / FetchS3Object) | `ListS3` toutes les 5 s (suivi par horodatage : chaque objet n'est listé qu'une fois), `FetchS3Object` sur `${s3.bucket}/${filename}` |
| Une ligne CSV → un événement JSON dans le bon topic | `UpdateAttribute` : `kafka.topic = raw-<dataset>` dérivé du chemin `<PAYS>/<dataset>/…` ; `UpdateRecord` (CSVReader → JsonRecordSetWriter) ; `PublishKafka` avec Record Reader/Writer = **un message par enregistrement**. Le pays est conservé dans chaque événement (`country_code` / `sender_country`) et en attribut `waba.country` |
| Enrichissement `ingestion_timestamp`, `source_file` | Ajoutés par `UpdateRecord` (+ `landed_at` = date de dépôt MinIO, qui sert à mesurer le lag de bout en bout) |
| Back-pressure | 10 000 flowfiles / 1 Go par connexion : quand `PublishKafka` ralentit, les files se remplissent et `ListS3` cesse de lister. `PublishKafka` en échec est rebouclé (nouvel essai avec pénalité) : rien n'est perdu si Kafka est indisponible |
| Fichier illisible | `UpdateRecord` → `failure` → `PublishKafka DLQ` (`dlq-financial-events`) |

Le CSV est lu en **champs texte** (« Use String Fields From Header ») : NiFi ne type ni ne
valide rien. La validation est centralisée dans Spark, ce qui évite deux jeux de règles.

Le flux est créé par `nifi/provision.py` (conteneur `nifi-init`) via l'API REST — pas de
clic manuel, reproductible, idempotent (un flux existant est conservé et redémarré). Les
propriétés sont résolues par nom interne **ou** libellé, et les types par suffixe de classe,
pour tolérer les évolutions de NiFi 2.x. Identifiants MinIO et Kafka : variables
d'environnement, stockés par NiFi dans des propriétés sensibles chiffrées
(`NIFI_SENSITIVE_PROPS_KEY`).

Par défaut (`NIFI_REPLAY_HISTORY=false`), seuls les fichiers déposés après le provisioning
sont publiés : l'historique (millions de lignes) est déjà traité par la couche batch.

## 3. Kafka (3.2)

Apache Kafka 4.3 en **KRaft** mono-nœud (pas de ZooKeeper), auto-création des topics
désactivée ; `kafka-init` crée les topics (3 partitions) :

| Topic | Couche | Rétention |
|---|---|---|
| `raw-bank-transactions`, `raw-insurance-operations`, `raw-mobile-money-payments`, `raw-loan-repayments` | Raw | 7 j |
| `silver-bank-transactions`, `silver-insurance-operations`, `silver-mobile-money`, `silver-loan-repayments`* | Silver | 7 j |
| `gold-fraud-alerts`, `gold-aml-events`, `gold-liquidity-alerts` | Gold | 90 j (audit conformité) |
| `dlq-financial-events` | DLQ | 30 j |

\* `silver-loan-repayments` n'est pas dans l'énoncé : ajouté pour que les 4 flux raw aient
leur équivalent Silver (sinon les remboursements seraient consommés sans sortie).

Les messages Silver/Gold ont pour **clé** l'identifiant métier (`transaction_id`,
`alert_id`…) : même partition pour un même événement, ordre garanti par clé.

## 4. Spark Structured Streaming (3.3)

Deux conteneurs dédiés (`stream-silver`, `stream-gold`), Spark en `local[2]` : les jobs
longue durée ne monopolisent pas les slots du cluster utilisés par les jobs batch
d'Airflow. `restart: unless-stopped` + checkpoints (volume `stream-checkpoints`) : après
un arrêt, chaque requête reprend exactement à ses offsets et à son état.

### Job 1 — raw → silver

| Étape | Détail |
|---|---|
| Lecture | 4 topics `raw-*`, `maxOffsetsPerTrigger` = 20 000 (back-pressure côté consommateur), micro-lot toutes les 5 s |
| Déduplication 10 min | `withWatermark(kafka_ts, "10 minutes")` + `dropDuplicatesWithinWatermark(topic, event_key)`. `event_key` = `transaction_id` / `operation_id` / `payment_id` / `repayment_id` ; pour un JSON illisible, ses coordonnées Kafka (sinon tous les messages malformés auraient la clé NULL et seraient fusionnés). Le temps de référence est l'horodatage Kafka (≈ ingestion NiFi) : monotone, il absorbe un fichier re-déposé dans les 10 min sans rejeter comme « tardifs » des événements métier anciens |
| Validation du schéma JSON | `from_json` en mode PERMISSIVE ; chaque champ est casté dans son type Bronze ; une valeur non convertible (montant `abc`) ou un JSON invalide → `MALFORMED_ROW`. Puis les règles du L1 : champs obligatoires, pays, entité, devise/pays, énumérations, montants ≥ 0, format des UUID |
| Transformations Silver | Fonctions du L2 : jointure aux référentiels (Bronze, en cache, rafraîchis toutes les 5 min), EUR au taux du mois, pseudonymisation SHA-256 salée, `sender_home_country` (pays du client émetteur, pour la fraude 2) |
| Double sink | 1) **Iceberg** `silver.rt_<dataset>` (MERGE sur la clé → un micro-lot rejoué après une panne ne duplique rien) ; 2) **Kafka** `silver-*`. Iceberg est écrit avant Kafka : quand le Job 2 reçoit un sinistre, la prime correspondante est déjà requêtable |
| DLQ | Rejets de validation (motif + message d'origine) et orphelins Silver (compte / client / agence inconnus) → `dlq-financial-events` ; rejets aussi historisés dans `audit.rejected_records`. Seuls les doublons exacts d'un même micro-lot ne vont pas en DLQ (ce n'est pas une erreur) |
| Logs | Une ligne JSON par micro-lot : `rows_in`, lignes Silver par flux, `dlq`, `quarantined`, `max_lag_s`, `lag_ok` (< 30 s) |

**Pourquoi `silver.rt_*` et pas `silver.*` directement ?** Le job batch Silver réécrit ses
partitions (pays × mois) de façon idempotente ; y fusionner le flux temps réel créerait des
conflits de commit Iceberg et ferait disparaître/réapparaître des lignes au gré des deux
écritures. C'est le schéma Lambda classique : la speed layer a son propre stockage, la
batch layer recalcule la vérité à partir de Bronze (qui reçoit les mêmes fichiers), et la
serving layer (Trino) combine les deux.

### Job 2 — silver → gold

Trois requêtes streaming dans le même driver, checkpoints séparés.

| Règle | Implémentation | Sortie |
|---|---|---|
| **Fraude 1** — transactions multiples > 500 000 XOF d'un même compte en < 5 min | Fenêtre glissante `window(timestamp, 5 min, 1 min)` par `account_key`, filtre `amount_eur > 762,25` (500 000 XOF au taux fixe), `count ≥ 2`, watermark 10 min, mode *update* (l'état de fenêtre traverse les micro-lots). GHS converti via l'EUR | `MULTIPLE_LARGE_TXN` |
| **Fraude 2** — paiement mobile money depuis un pays inhabituel | `sender_country ≠ sender_home_country` (pays de résidence du client dans le référentiel) | `UNUSUAL_COUNTRY` |
| **Fraude 3** — sinistre > 3 × prime annuelle | Primes (`PREMIUM_PAYMENT`, `POLICY_RENEWAL`) des 365 derniers jours par police, lues dans `silver.insurance_operations` ∪ `silver.rt_insurance_operations` ∪ micro-lot courant | `CLAIM_EXCEEDS_PREMIUM` |
| **AML** | Virement bancaire (`TRANSFER`, `INTERNATIONAL_WIRE`) ou mobile money (`P2P`, `CROSS_BORDER_TRANSFER`) ≥ seuil déclaratif **en devise locale** : 1 000 000 XOF (UEMOA), 5 000 GHS (Ghana) | `gold-aml-events` |
| **Liquidité** | Fenêtre glissante 5 min / 1 min par pays : sorties nettes (retraits + virements − dépôts réussis). Réserve de référence = 3 % des dépôts à vue et d'épargne actifs (`silver.accounts`, taux de réserves obligatoires BCEAO) ; alerte si les sorties nettes > 50 % de la réserve | `gold-liquidity-alerts` |

Chaque alerte : `alert_id` déterministe (SHA-256 de la règle + du sujet + de l'instant), règle,
sévérité, pays, sujet pseudonymisé, fenêtre, montants EUR et locaux, seuil, `details` JSON.
Elle est publiée dans Kafka **et** fusionnée (MERGE sur `alert_id`) dans une table Iceberg
Gold, requêtable dans Trino et historisée.

**Anti-rafale** : une rafale est vue par jusqu'à 5 fenêtres glissantes. L'`alert_id` de la
fraude 1 est calculé sur la *première* transaction de la rafale, une seule alerte est gardée
par (règle, sujet) dans un micro-lot, et une alerte ≤ 5 min d'une alerte déjà émise pour le
même sujet est supprimée. Une mise à jour de la même alerte (3e transaction de la rafale)
est ré-émise et met à jour la ligne Iceberg.

Les seuils sont des constantes de `spark/jobs/common/streaming.py`, partagées avec le
générateur de scénarios.

## 5. Accès unifié via Trino (3.4)

Catalogue `kafka` (`trino/etc/catalog/kafka.properties`) + une description JSON par topic
(`trino/etc/kafka/*.json`) qui déclare les colonnes du message. Pour
`silver-bank-transactions`, la colonne JSON `timestamp` est exposée comme `event_time`
(TIMESTAMP) et `amount_eur` aussi comme `streaming_amount_eur` : la requête de l'énoncé
s'exécute **telle quelle** (`sql/07_lambda.sql`, `make lambda`), avec
`gold.daily_transaction_volume (country_code, txn_date, total_amount_eur)`.

`07_lambda.sql` propose aussi une variante agrégée : la requête de l'énoncé joint un
agrégat journalier à des messages unitaires (le montant batch est répété par message) ;
la variante agrège les deux côtés et ne prend côté Kafka que les jours postérieurs au
dernier jour présent en Gold, pour éviter tout double comptage.

## 6. Sécurité et conformité

- Aucun secret dans le code : NiFi, MinIO, Kafka, secret de pseudonymisation via `.env`.
- Les topics `silver-*` et `gold-*` ne contiennent **aucun identifiant en clair** (clés
  SHA-256 salées) ; seuls les topics `raw-*` (7 jours) contiennent les données brutes,
  comme le bucket `raw-landing`.
- Kafka UI n'a pas d'authentification : exposée sur 127.0.0.1 uniquement. Kafka est en
  PLAINTEXT (réseau Docker interne) : en production, SASL/SCRAM + TLS et ACL par topic
  (Level 4).

## 7. Tests

| Test | Couverture |
|---|---|
| `test_event_key_and_parse_raw` | JSON NiFi → types Bronze, `""` → NULL, montant non numérique / JSON invalide → `MALFORMED_ROW`, devise incohérente, clé de dédup de repli |
| `test_streaming_dedup_within_10_minutes` | Vrai streaming (source fichiers) : rejeu à +5 min absorbé, même clé à +31 min conservée |
| `test_streaming_sliding_window_bursts` | Vrai streaming : rafale répartie sur 2 micro-lots reconstituée par l'état de fenêtre, une seule alerte |
| `test_job1_writes_silver_rt_and_dlq` / `test_job2_raises_every_scenario_alert` | Bout en bout : scénarios du générateur → Job 1 → JSON `silver-*` → Job 2 : les 5 alertes, XOF et GHS, DLQ (malformé + orphelin), pas d'identifiant en clair, rejeu idempotent |
| `nifi/tests/test_provision.py` | Résolution des propriétés/valeurs NiFi, routage, et création complète du flux contre un faux serveur REST |

Non couvert automatiquement : l'exécution réelle de NiFi, du broker Kafka et des écritures
Iceberg des jobs streaming (vérifiée par `make demo-l3` puis `make streaming-check`).
