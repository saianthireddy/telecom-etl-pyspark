"""Command line.

python -m telecom_etl generate --root data --batches 3
python -m telecom_etl run --root data
python -m telecom_etl run --root data --reprocess 2
"""

from __future__ import annotations

import argparse
import json

from .generate import generate
from .pipeline import QualityGateError, run
from .spark import get_spark


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="telecom_etl")
    sub = parser.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate", help="write synthetic landing batches")
    g.add_argument("--root", default="data")
    g.add_argument("--batches", type=int, default=3)
    g.add_argument("--subscribers", type=int, default=200)
    g.add_argument("--cdrs-per-day", type=int, default=1000)
    g.add_argument("--seed", type=int, default=7)

    r = sub.add_parser("run", help="process batches after the watermark")
    r.add_argument("--root", default="data")
    r.add_argument("--reprocess", type=int, nargs="*", help="re-run these batches")
    r.add_argument("--max-reject-rate", type=float, default=0.05)

    args = parser.parse_args(argv)
    if args.command == "generate":
        manifest = generate(args.root, args.batches, args.subscribers, args.cdrs_per_day, args.seed)
        print(json.dumps(manifest["batches"], indent=2))
        return 0

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        reports = run(spark, args.root, args.reprocess, args.max_reject_rate)
    except QualityGateError as exc:
        print(f"FAILED: {exc}")
        return 1
    finally:
        spark.stop()
    if not reports:
        print("nothing to do: no batches after the watermark")
    for report in reports:
        print(json.dumps(report, indent=2))
    return 0
