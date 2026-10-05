"""Bundle d'import Superset (format d'export v1) des 3 dashboards WABA — Level 4.

Tout est décrit en Python (pas d'export manuel à maintenir) : base Trino, datasets
virtuels (SQL sur les couches Silver / Gold d'Iceberg), graphiques et dashboards.
Les UUID sont déterministes (uuid5) : réimporter met à jour au lieu de dupliquer.

Chaque dataset expose `country_code` : le filtre natif « Pays » de chaque dashboard
s'applique à tous ses graphiques.
"""
from __future__ import annotations

import uuid

import yaml

NS = uuid.UUID("5d0e6c1e-4a0b-4c55-9d6b-0f2a57c0ba5e")
VERSION = "1.0.0"
DB_FILE = "databases/Trino_WABA.yaml"
SLA_DAYS = 30        # SLA contractuel de règlement d'un sinistre (jours)
CIMA_LOSS_RATIO = 70  # seuil CIMA (%)


def uid(*parts: str) -> str:
    return str(uuid.uuid5(NS, "/".join(parts)))


# --------------------------------------------------------------------------- base
def database(trino_uri: str) -> dict:
    return {
        "database_name": "Trino WABA",
        "sqlalchemy_uri": trino_uri,
        "cache_timeout": 600,
        "expose_in_sqllab": True,
        "allow_run_async": False,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
        "allow_csv_upload": False,
        # Les requêtes partent sous l'identité de l'utilisateur connecté (SSO) :
        # Trino applique ses règles (filtre pays, masques, tables réglementaires).
        "impersonate_user": True,
        "extra": {
            "engine_params": {"connect_args": {"http_scheme": "https", "verify": False}},
            "metadata_params": {},
            "schemas_allowed_for_file_upload": [],
            "allows_virtual_table_explore": True,
        },
        "uuid": uid("database", "trino"),
        "version": VERSION,
    }


# --------------------------------------------------------------------------- datasets
def col(name: str, typ: str = "VARCHAR", dttm: bool = False, verbose: str | None = None) -> dict:
    return {"column_name": name, "verbose_name": verbose, "is_dttm": dttm, "is_active": True,
            "type": typ, "groupby": True, "filterable": True, "expression": None,
            "description": None, "python_date_format": None, "extra": None}


def metric(name: str, expr: str, verbose: str, fmt: str | None = None) -> dict:
    return {"metric_name": name, "verbose_name": verbose, "metric_type": None,
            "expression": expr, "description": None, "d3format": fmt, "extra": None,
            "warning_text": None}


DATASETS: dict[str, dict] = {
    # ------------------------------------------------ Dashboard 1 : performance commerciale
    "revenus_ligne_metier": {
        "description": "Revenus mensuels par pays et ligne métier (commissions bancaires et "
                       "intérêts perçus, primes d'assurance, frais mobile money), en EUR",
        "sql": """
SELECT CAST(date_trunc('month', "timestamp") AS date) AS month, country_code,
       'Banque' AS business_line, sum(fee_amount_eur) AS revenue_eur
FROM silver.bank_transactions WHERE is_success AND NOT is_aberrant GROUP BY 1, 2
UNION ALL
SELECT event_month, country_code, 'Banque', sum(interest_received_eur)
FROM silver.loan_repayments WHERE NOT is_aberrant GROUP BY 1, 2
UNION ALL
SELECT op_month, country_code, 'Assurance', sum(amount_eur)
FROM silver.insurance_operations WHERE is_premium AND NOT is_aberrant GROUP BY 1, 2
UNION ALL
SELECT CAST(date_trunc('month', "timestamp") AS date), country_code, 'Mobile Money',
       sum(fee_amount_eur)
FROM silver.mobile_money_payments WHERE is_success AND NOT is_aberrant GROUP BY 1, 2""",
        "dttm": "month",
        "columns": [col("month", "DATE", True, "Mois"), col("country_code", verbose="Pays"),
                    col("business_line", verbose="Ligne métier"),
                    col("revenue_eur", "DECIMAL", verbose="Revenus (EUR)")],
        "metrics": [metric("revenus_eur", "SUM(revenue_eur)", "Revenus (EUR)", ",.0f")],
    },
    "arpc_mensuel": {
        "description": "ARPC : revenu moyen par client actif et par mois (gold.customer_arpu_monthly)",
        "sql": """
SELECT month, country_code, customer_segment, revenue_eur, active_customers
FROM gold.customer_arpu_monthly""",
        "dttm": "month",
        "columns": [col("month", "DATE", True, "Mois"), col("country_code", verbose="Pays"),
                    col("customer_segment", verbose="Segment"),
                    col("revenue_eur", "DECIMAL"), col("active_customers", "BIGINT")],
        "metrics": [metric("arpc_eur", "SUM(revenue_eur) / NULLIF(SUM(active_customers), 0)",
                           "ARPC (EUR)", ",.2f")],
    },
    "top_produits": {
        "description": "Top 10 des produits les plus souscrits par pays (banque et assurance)",
        "sql": """
SELECT country_code, entity_type, product_name, nb_souscriptions, rang
FROM (
  SELECT country_code, entity_type, product_name, count(*) AS nb_souscriptions,
         row_number() OVER (PARTITION BY country_code ORDER BY count(*) DESC) AS rang
  FROM silver.accounts
  WHERE entity_type IN ('BANK', 'INSURANCE') AND status = 'ACTIVE'
  GROUP BY 1, 2, 3
) t
WHERE rang <= 10""",
        "dttm": None,
        "columns": [col("country_code", verbose="Pays"), col("entity_type", verbose="Entité"),
                    col("product_name", verbose="Produit"),
                    col("nb_souscriptions", "BIGINT", verbose="Souscriptions"),
                    col("rang", "BIGINT", verbose="Rang")],
        "metrics": [metric("souscriptions", "SUM(nb_souscriptions)", "Souscriptions", ",d")],
    },
    # ------------------------------------------------ Dashboard 2 : risque & conformité
    "npl_pays": {
        "description": "Taux de créances douteuses (NPL) par pays, dernier arrêté (seuil BCEAO 5 %)",
        "sql": """
SELECT country_code, as_of_date, round(npl_ratio * 100, 2) AS npl_pct, nb_loans,
       nb_loans_npl, outstanding_total_eur, outstanding_npl_eur
FROM gold.npl_ratio_by_country
WHERE loan_type = 'ALL'
  AND as_of_date = (SELECT max(as_of_date) FROM gold.npl_ratio_by_country)""",
        "dttm": None,
        "columns": [col("country_code", verbose="Pays"), col("as_of_date", "DATE"),
                    col("npl_pct", "DOUBLE", verbose="NPL (%)"), col("nb_loans", "BIGINT"),
                    col("nb_loans_npl", "BIGINT"), col("outstanding_total_eur", "DECIMAL"),
                    col("outstanding_npl_eur", "DECIMAL")],
        "metrics": [metric("npl_pct_pondere",
                           "100.0 * SUM(outstanding_npl_eur) / NULLIF(SUM(outstanding_total_eur), 0)",
                           "NPL (%)", ",.2f")],
    },
    "loss_ratio": {
        "description": f"Loss ratio (sinistres payés / primes) par produit et pays, seuil CIMA {CIMA_LOSS_RATIO} %",
        "sql": f"""
SELECT country_code, product_line, line_family,
       round(100.0 * sum(claims_paid_eur) / nullif(sum(premiums_eur), 0), 1) AS loss_ratio_pct,
       {CIMA_LOSS_RATIO} AS seuil_cima_pct,
       sum(premiums_eur) AS premiums_eur, sum(claims_paid_eur) AS claims_paid_eur
FROM gold.loss_ratio_by_product
GROUP BY 1, 2, 3""",
        "dttm": None,
        "columns": [col("country_code", verbose="Pays"), col("product_line", verbose="Produit"),
                    col("line_family", verbose="Famille"),
                    col("loss_ratio_pct", "DOUBLE", verbose="Loss ratio (%)"),
                    col("seuil_cima_pct", "INTEGER", verbose="Seuil CIMA (%)"),
                    col("premiums_eur", "DECIMAL"), col("claims_paid_eur", "DECIMAL")],
        "metrics": [metric("loss_ratio", "100.0 * SUM(claims_paid_eur) / NULLIF(SUM(premiums_eur), 0)",
                           "Loss ratio (%)", ",.1f"),
                    metric("seuil_cima", f"MAX({CIMA_LOSS_RATIO})", "Seuil CIMA (%)", ",d")],
    },
    "alertes_aml_30j": {
        "description": "Alertes AML (virements > seuil déclaratif) par jour et pays, 30 derniers jours",
        "sql": """
SELECT CAST(event_time AS date) AS alert_date, country_code, currency,
       count(*) AS nb_alertes, sum(amount_eur) AS montant_eur
FROM gold.aml_events
WHERE event_time >= current_date - INTERVAL '30' DAY
GROUP BY 1, 2, 3""",
        "dttm": "alert_date",
        "columns": [col("alert_date", "DATE", True, "Jour"), col("country_code", verbose="Pays"),
                    col("currency"), col("nb_alertes", "BIGINT", verbose="Alertes"),
                    col("montant_eur", "DOUBLE")],
        "metrics": [metric("alertes", "SUM(nb_alertes)", "Alertes AML", ",d")],
    },
    "delais_sinistres": {
        "description": f"Délai moyen de traitement des sinistres vs SLA contractuel ({SLA_DAYS} jours)",
        "sql": f"""
SELECT month, country_code, line_family, nb_claims, avg_processing_days,
       p90_processing_days, {SLA_DAYS} AS sla_days
FROM gold.claims_processing_time""",
        "dttm": "month",
        "columns": [col("month", "DATE", True, "Mois"), col("country_code", verbose="Pays"),
                    col("line_family", verbose="Famille"), col("nb_claims", "BIGINT"),
                    col("avg_processing_days", "DOUBLE"), col("p90_processing_days", "DOUBLE"),
                    col("sla_days", "INTEGER")],
        "metrics": [metric("delai_moyen",
                           "SUM(avg_processing_days * nb_claims) / NULLIF(SUM(nb_claims), 0)",
                           "Délai moyen (jours)", ",.1f"),
                    metric("sla", f"MAX({SLA_DAYS})", "SLA (jours)", ",d")],
    },
    # ------------------------------------------------ Dashboard 3 : mobile money
    "mm_par_heure": {
        "description": "Paiements mobile money par pays et heure de la journée (UTC)",
        "sql": """
SELECT country_code, hour("timestamp") AS heure, count(*) AS nb_paiements,
       sum(amount_eur) AS montant_eur
FROM silver.mobile_money_payments
GROUP BY 1, 2""",
        "dttm": None,
        "columns": [col("country_code", verbose="Pays"), col("heure", "BIGINT", verbose="Heure"),
                    col("nb_paiements", "BIGINT"), col("montant_eur", "DECIMAL")],
        "metrics": [metric("paiements", "SUM(nb_paiements)", "Paiements", ",d")],
    },
    "corridors": {
        "description": "Transferts transfrontaliers par corridor (banque + mobile money)",
        "sql": """
SELECT corridor, sender_country AS country_code, receiver_country, channel,
       sum(nb_transfers) AS nb_transfers, sum(amount_total_eur) AS montant_eur
FROM gold.cross_border_transfers
WHERE sender_country <> receiver_country
GROUP BY 1, 2, 3, 4""",
        "dttm": None,
        "columns": [col("corridor", verbose="Corridor"), col("country_code", verbose="Pays émetteur"),
                    col("receiver_country"), col("channel", verbose="Canal"),
                    col("nb_transfers", "BIGINT"), col("montant_eur", "DECIMAL")],
        "metrics": [metric("montant_corridor", "SUM(montant_eur)", "Montant (EUR)", ",.0f")],
    },
    "mm_echecs": {
        "description": "Taux d'échec des paiements mobile money par opérateur et pays",
        "sql": """
SELECT country_code, operator, count(*) AS nb_paiements,
       sum(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS nb_echecs
FROM silver.mobile_money_payments
GROUP BY 1, 2""",
        "dttm": None,
        "columns": [col("country_code", verbose="Pays"), col("operator", verbose="Opérateur"),
                    col("nb_paiements", "BIGINT"), col("nb_echecs", "BIGINT")],
        "metrics": [metric("taux_echec", "100.0 * SUM(nb_echecs) / NULLIF(SUM(nb_paiements), 0)",
                           "Taux d'échec (%)", ",.2f")],
    },
}


def dataset(name: str) -> dict:
    d = DATASETS[name]
    return {
        "table_name": name, "main_dttm_col": d["dttm"], "description": d["description"],
        "default_endpoint": None, "offset": 0, "cache_timeout": None, "catalog": "iceberg",
        "schema": "gold", "sql": d["sql"].strip(), "params": None, "template_params": None,
        "filter_select_enabled": True, "fetch_values_predicate": None,
        "extra": None, "normalize_columns": False, "always_filter_main_dttm": False,
        "uuid": uid("dataset", name), "metrics": d["metrics"], "columns": d["columns"],
        "version": VERSION, "database_uuid": uid("database", "trino"),
    }


# --------------------------------------------------------------------------- graphiques
def _bar(x: str, metrics: list[str], groupby: list[str] | None = None, *, horizontal=False,
         limit: int | None = None, sort_metric: str | None = None, y_format="SMART_NUMBER",
         stack=False) -> dict:
    p = {"viz_type": "echarts_timeseries_bar", "x_axis": x, "metrics": metrics,
         "groupby": groupby or [], "adhoc_filters": [], "row_limit": limit or 10000,
         "orientation": "horizontal" if horizontal else "vertical", "show_legend": True,
         "legendOrientation": "top", "legendType": "scroll", "rich_tooltip": True,
         "y_axis_format": y_format, "color_scheme": "supersetColors",
         "x_axis_sort_asc": True, "truncate_metric": True, "show_empty_columns": True,
         "stack": "Stack" if stack else None}
    if sort_metric:
        p.update(timeseries_limit_metric=sort_metric, order_desc=True,
                 x_axis_sort=sort_metric, x_axis_sort_asc=False)
    return p


def _line(x: str, metric: str, groupby: list[str], grain: str) -> dict:
    return {"viz_type": "echarts_timeseries_line", "x_axis": x, "time_grain_sqla": grain,
            "metrics": [metric], "groupby": groupby, "adhoc_filters": [
                {"clause": "WHERE", "comparator": "No filter", "expressionType": "SIMPLE",
                 "operator": "TEMPORAL_RANGE", "subject": x}],
            "row_limit": 10000, "show_legend": True, "legendOrientation": "top",
            "legendType": "scroll", "rich_tooltip": True, "markerEnabled": True,
            "y_axis_format": "SMART_NUMBER", "color_scheme": "supersetColors",
            "x_axis_time_format": "smart_date"}


def _table(columns: list[str], order: list[str], formatting: list[dict] | None = None,
           limit: int = 1000) -> dict:
    return {"viz_type": "table", "query_mode": "raw", "all_columns": columns,
            "order_by_cols": [f'["{c}", true]' for c in order], "adhoc_filters": [],
            "row_limit": limit, "server_pagination": False, "include_search": True,
            "show_cell_bars": False, "conditional_formatting": formatting or []}


def _fmt(column: str, operator: str, color: str, value=None, left=None, right=None) -> dict:
    f = {"column": column, "operator": operator, "colorScheme": color, "useGradient": False}
    if value is not None:
        f["targetValue"] = value
    if left is not None:
        f.update(targetValueLeft=left, targetValueRight=right)
    return f


CHARTS: dict[str, dict] = {
    # D1
    "Revenus par pays et ligne métier": {
        "dataset": "revenus_ligne_metier",
        "params": _bar("country_code", ["revenus_eur"], ["business_line"]),
        "description": "Revenus consolidés (EUR) : Banque / Assurance / Mobile Money"},
    "ARPC mensuel par pays": {
        "dataset": "arpc_mensuel", "params": _line("month", "arpc_eur", ["country_code"], "P1M"),
        "description": "Revenu moyen par client actif (EUR / mois)"},
    "Contribution des pays aux revenus du groupe": {
        "dataset": "revenus_ligne_metier",
        "viz_type": "world_map",
        "params": {"viz_type": "world_map", "entity": "country_code", "country_fieldtype": "cca2",
                   "metric": "revenus_eur", "show_bubbles": False, "adhoc_filters": [],
                   "row_limit": 50, "linear_color_scheme": "schemeOranges",
                   "color_by": "metric", "max_bubble_size": "25"},
        "description": "Carte choroplèthe : part de chaque pays dans les revenus du groupe"},
    "Top 10 des produits souscrits par pays": {
        "dataset": "top_produits",
        "params": _table(["country_code", "rang", "product_name", "entity_type",
                          "nb_souscriptions"], ["country_code", "rang"], limit=500),
        "description": "Comptes / polices actifs par produit (banque et assurance)"},
    # D2
    "Taux NPL par pays": {
        "dataset": "npl_pays",
        "params": _table(["country_code", "npl_pct", "nb_loans", "nb_loans_npl",
                          "outstanding_total_eur", "as_of_date"], ["country_code"], formatting=[
            _fmt("npl_pct", "<", "colorSuccess", value=3),
            _fmt("npl_pct", "≤ x ≤", "colorWarning", left=3, right=5),
            _fmt("npl_pct", ">", "colorError", value=5)]),
        "description": "Vert < 3 %, orange 3-5 %, rouge > 5 % (seuil BCEAO)"},
    "NPL (%) par pays": {
        "dataset": "npl_pays", "params": _bar("country_code", ["npl_pct_pondere"]),
        "description": "Encours douteux / encours total"},
    "Loss ratio par produit et pays (seuil CIMA 70 %)": {
        "dataset": "loss_ratio",
        "params": _bar("product_line", ["loss_ratio"], ["country_code"]),
        "description": "Sinistres payés / primes ; au-delà de 70 % : alerte CIMA"},
    "Loss ratio vs seuil CIMA": {
        "dataset": "loss_ratio",
        "params": _table(["country_code", "product_line", "line_family", "loss_ratio_pct",
                          "seuil_cima_pct"], ["country_code", "product_line"], formatting=[
            _fmt("loss_ratio_pct", ">", "colorError", value=CIMA_LOSS_RATIO),
            _fmt("loss_ratio_pct", "≤", "colorSuccess", value=CIMA_LOSS_RATIO)]),
        "description": "Rouge : au-dessus du seuil CIMA"},
    "Alertes AML par jour et par pays (30 jours)": {
        "dataset": "alertes_aml_30j",
        "params": _line("alert_date", "alertes", ["country_code"], "P1D"),
        "description": "Événements gold-aml-events (Level 3)"},
    "Délai de traitement des sinistres vs SLA": {
        "dataset": "delais_sinistres",
        "params": _bar("country_code", ["delai_moyen", "sla"]),
        "description": f"Délai moyen de règlement (jours) comparé au SLA contractuel de {SLA_DAYS} jours"},
    # D3
    "Paiements mobile money par pays et par heure": {
        "dataset": "mm_par_heure", "viz_type": "heatmap_v2",
        "params": {"viz_type": "heatmap_v2", "x_axis": "heure", "groupby": "country_code",
                   "metric": "paiements", "adhoc_filters": [], "row_limit": 10000,
                   "linear_color_scheme": "schemeBlues", "normalize_across": "heatmap",
                   "sort_x_axis": "alpha_asc", "sort_y_axis": "alpha_asc",
                   "show_legend": True, "show_percentage": False, "show_values": False,
                   "value_bounds": [None, None], "y_axis_format": "SMART_NUMBER",
                   "xscale_interval": None, "yscale_interval": None},
        "description": "Heatmap pays x heure (UTC)"},
    "Top 5 des corridors transfrontaliers": {
        "dataset": "corridors",
        "params": _bar("corridor", ["montant_corridor"], horizontal=True, limit=5,
                       sort_metric="montant_corridor"),
        "description": "Montant transféré (EUR) par corridor : CI→SN, CI→ML, ..."},
    "Taux d'échec par opérateur et pays": {
        "dataset": "mm_echecs", "params": _bar("operator", ["taux_echec"], ["country_code"]),
        "description": "Paiements FAILED / total (%)"},
}

DASHBOARDS: dict[str, dict] = {
    "performance-commerciale": {
        "title": "WABA — Performance Commerciale Groupe",
        "filter_dataset": "revenus_ligne_metier",
        "rows": [["Revenus par pays et ligne métier", "Contribution des pays aux revenus du groupe"],
                 ["ARPC mensuel par pays", "Top 10 des produits souscrits par pays"]],
    },
    "risque-conformite": {
        "title": "WABA — Risque & Conformité Réglementaire",
        "filter_dataset": "npl_pays",
        "rows": [["Taux NPL par pays", "NPL (%) par pays"],
                 ["Loss ratio par produit et pays (seuil CIMA 70 %)", "Loss ratio vs seuil CIMA"],
                 ["Alertes AML par jour et par pays (30 jours)",
                  "Délai de traitement des sinistres vs SLA"]],
    },
    "mobile-money": {
        "title": "WABA — Mobile Money & Transferts",
        "filter_dataset": "mm_par_heure",
        "rows": [["Paiements mobile money par pays et par heure"],
                 ["Top 5 des corridors transfrontaliers", "Taux d'échec par opérateur et pays"]],
    },
}

# Datasets visibles par rôle Superset (en plus, Trino applique ses propres règles)
ROLE_DATASETS = {
    "WABA_Analyst": list(DATASETS),
    "WABA_Compliance": ["npl_pays", "loss_ratio", "alertes_aml_30j", "delais_sinistres"],
    "WABA_Viewer": [d for d in DATASETS if d != "alertes_aml_30j"],
}


def chart(name: str, chart_id: int) -> dict:
    c = CHARTS[name]
    viz = c.get("viz_type", c["params"]["viz_type"])
    return {"slice_name": name, "description": c.get("description"), "certified_by": None,
            "certification_details": None, "viz_type": viz,
            "params": {**c["params"], "datasource": f"{chart_id}__table"},
            "query_context": None, "cache_timeout": None,
            "uuid": uid("chart", name), "version": VERSION,
            "dataset_uuid": uid("dataset", c["dataset"])}


def dashboard(slug: str, chart_ids: dict[str, int]) -> dict:
    d = DASHBOARDS[slug]
    position = {"DASHBOARD_VERSION_KEY": "v2",
                "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
                "GRID_ID": {"id": "GRID_ID", "type": "GRID", "children": [],
                            "parents": ["ROOT_ID"]},
                "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER",
                              "meta": {"text": d["title"]}}}
    for r, row in enumerate(d["rows"]):
        row_id = f"ROW-{slug}-{r}"
        position["GRID_ID"]["children"].append(row_id)
        position[row_id] = {"id": row_id, "type": "ROW", "children": [],
                            "parents": ["ROOT_ID", "GRID_ID"],
                            "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        width = 12 // len(row)
        for c, name in enumerate(row):
            cid = f"CHART-{slug}-{r}-{c}"
            position[row_id]["children"].append(cid)
            position[cid] = {"id": cid, "type": "CHART", "children": [],
                             "parents": ["ROOT_ID", "GRID_ID", row_id],
                             "meta": {"chartId": chart_ids[name], "height": 60, "width": width,
                                      "sliceName": name, "uuid": uid("chart", name)}}
    native_filter = {
        "id": f"NATIVE_FILTER-pays-{slug}", "name": "Pays", "filterType": "filter_select",
        "type": "NATIVE_FILTER", "description": "Filtre tous les graphiques par pays",
        "targets": [{"column": {"name": "country_code"},
                     "datasetUuid": uid("dataset", d["filter_dataset"])}],
        "controlValues": {"multiSelect": True, "enableEmptyFilter": False,
                          "defaultToFirstItem": False, "inverseSelection": False,
                          "searchAllOptions": False},
        "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
        "cascadeParentIds": [], "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
    }
    return {"dashboard_title": d["title"], "description": None, "css": "", "slug": slug,
            "uuid": uid("dashboard", slug), "position": position,
            "metadata": {"native_filter_configuration": [native_filter],
                         "color_scheme": "supersetColors", "cross_filters_enabled": True,
                         "refresh_frequency": 0, "expanded_slices": {},
                         "timed_refresh_immune_slices": [], "default_filters": "{}"},
            "version": VERSION, "published": True}


def bundle(trino_uri: str) -> dict[str, str]:
    """Contenu de l'archive d'import : {chemin: YAML}."""
    chart_ids = {name: 1000 + i for i, name in enumerate(CHARTS)}
    files = {"metadata.yaml": {"version": VERSION, "type": "Dashboard",
                               "timestamp": "2026-09-30T00:00:00+00:00"},
             DB_FILE: database(trino_uri)}
    for name in DATASETS:
        files[f"datasets/Trino_WABA/{name}.yaml"] = dataset(name)
    for name, cid in chart_ids.items():
        files[f"charts/{uid('chart', name)[:8]}.yaml"] = chart(name, cid)
    for slug in DASHBOARDS:
        files[f"dashboards/{slug}.yaml"] = dashboard(slug, chart_ids)
    return {path: yaml.safe_dump(content, allow_unicode=True, sort_keys=False)
            for path, content in files.items()}
