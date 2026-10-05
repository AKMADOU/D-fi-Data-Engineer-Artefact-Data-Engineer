"""CLI du générateur (mode headless, utile pour les scripts, la CI et Airflow au L2).

Exemples :
    python -m waba_gen.cli referentials                    # 500k clients / 800k comptes
    python -m waba_gen.cli referentials --customers 50000 --accounts 80000
    python -m waba_gen.cli events --dataset all            # volumes par défaut, dernier trimestre
    python -m waba_gen.cli events --dataset bank_transactions --rows 2000 --countries CI SN
    python -m waba_gen.cli continuous --dataset mobile_money_payments --rows 200 --iterations 5
    python -m waba_gen.cli scenarios --names burst aml unusual_country big_claim bank_run
"""
from __future__ import annotations

import argparse
import json
import logging
import random
import sys
import time
from datetime import datetime

from . import config as C
from .scenarios import SCENARIOS
from .service import (continuous_tick, generate_dataset, generate_fraud_scenarios,
                      generate_referentials, get_sink, load_store)
from .transactions import GenerationError, default_period


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO, stream=sys.stdout,
        format='{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s",'
               '"msg":"%(message)s"}')


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="waba-gen", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--countries", nargs="+", default=C.COUNTRIES, choices=C.COUNTRIES)
    common.add_argument("--entities", nargs="+", default=C.ENTITY_TYPES,
                        choices=C.ENTITY_TYPES)

    r = sub.add_parser("referentials", parents=[common], help="Référentiels complets")
    r.add_argument("--customers", type=int, default=C.DEFAULT_REFERENTIAL_SIZES["customers"])
    r.add_argument("--accounts", type=int, default=C.DEFAULT_REFERENTIAL_SIZES["accounts"])
    r.add_argument("--branches", type=int, default=C.DEFAULT_REFERENTIAL_SIZES["branches"])
    r.add_argument("--products", type=int, default=C.DEFAULT_REFERENTIAL_SIZES["products"])
    r.add_argument("--seed", type=int, default=42)

    start, end = default_period()
    e = sub.add_parser("events", parents=[common], help="Transactions (one-time)")
    e.add_argument("--dataset", default="all", choices=["all", *C.EVENT_DATASETS])
    e.add_argument("--rows", type=int, help="Nombre de lignes (défaut : valeurs de l'énoncé)")
    e.add_argument("--start", type=_parse_date, default=start)
    e.add_argument("--end", type=_parse_date, default=end)
    e.add_argument("--seed", type=int)
    e.add_argument("--anomaly-rate", type=float, default=0.0)

    c = sub.add_parser("continuous", parents=[common], help="Flux continu (micro-lots)")
    c.add_argument("--dataset", required=True, choices=["referentials", *C.EVENT_DATASETS])
    c.add_argument("--rows", type=int, default=200, help="Lignes par micro-lot")
    c.add_argument("--min-interval", type=int, default=10)
    c.add_argument("--max-interval", type=int, default=60)
    c.add_argument("--iterations", type=int, default=0, help="0 = infini")
    c.add_argument("--anomaly-rate", type=float, default=0.0)

    f = sub.add_parser("scenarios", parents=[common],
                       help="Level 3 : scénarios de fraude / AML / liquidité (horodatés maintenant)")
    f.add_argument("--names", nargs="+", default=list(SCENARIOS), choices=list(SCENARIOS))
    return p


def main(argv: list[str] | None = None) -> int:
    _setup_logging()
    log = logging.getLogger("waba_gen.cli")
    args = build_parser().parse_args(argv)
    sink = get_sink()
    try:
        if args.command == "referentials":
            sizes = {"customers": args.customers, "accounts": args.accounts,
                     "branches": args.branches, "products": args.products}
            _, res = generate_referentials(sink, sizes, args.seed, args.countries,
                                           args.entities)
            log.info(json.dumps({"rows": res.rows, "files": res.files}))
            return 0

        store = load_store()
        if args.command == "scenarios":
            for res in generate_fraud_scenarios(store, sink, args.names, args.countries):
                log.info(json.dumps({"dataset": res.dataset, "rows": res.rows,
                                     "files": res.files}))
            return 0
        if args.command == "events":
            datasets = list(C.EVENT_DATASETS) if args.dataset == "all" else [args.dataset]
            for ds in datasets:
                res = generate_dataset(ds, store, sink, args.rows or C.DEFAULT_ROWS[ds],
                                       args.countries, args.entities, args.start, args.end,
                                       seed=args.seed, anomaly_rate=args.anomaly_rate)
                log.info(json.dumps({"dataset": ds, "rows": res.rows,
                                     "files": len(res.files), "anomalies": res.anomalies}))
            return 0

        # continuous
        if not 1 <= args.min_interval <= args.max_interval:
            raise GenerationError("Intervalle invalide")
        i = 0
        while args.iterations == 0 or i < args.iterations:
            wait = random.randint(args.min_interval, args.max_interval)
            res = continuous_tick(args.dataset, store, sink, args.rows, args.countries,
                                  args.entities, window_seconds=wait,
                                  anomaly_rate=args.anomaly_rate)
            i += 1
            log.info(json.dumps({"tick": i, "dataset": args.dataset, "rows": res.rows,
                                 "files": res.files, "next_in_s": wait}))
            if args.iterations == 0 or i < args.iterations:
                time.sleep(wait)
        return 0
    except GenerationError as exc:
        log.error(str(exc))
        return 2


if __name__ == "__main__":
    sys.exit(main())
