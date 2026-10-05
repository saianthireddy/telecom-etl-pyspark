"""Incremental pipeline: process every landed batch after the watermark.

Per batch, in order:
  1. bronze   cast landing CSVs, quarantine unreadable rows
  2. dim      apply the subscriber CDC batch to the SCD2 dimension
  3. silver   validate CDRs against the dimension *as of call time*, dedupe,
              merge into event-date partitions (late records land in old days)
  4. gold     recompute daily usage for exactly the dates the batch touched
  5. gate     fail the batch if too many rows were rejected
  6. commit   advance the watermark and point at the new dimension snapshot

The watermark (``_state.json``) moves only after every step succeeded, so a
crash mid-batch means the batch is retried next run. Every write either
overwrites a partition keyed by the batch or by event date, or writes a new
dimension snapshot, so a retried batch produces the same lake as a clean run.

Layout under ``root/lake``:
  bronze/cdr/batch=N                   bronze/cdr_quarantine/batch=N
  bronze/subscriber_cdc/batch=N        silver/cdr/event_date=YYYY-MM-DD
  silver/cdr_rejects/batch=N           silver/dim_subscriber/v=K  (snapshot K)
  gold/daily_usage/event_date=...      reports/batch=N.json
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import bronze as B
from . import gold as G
from . import silver as S
from .scd2 import apply_cdc


class QualityGateError(RuntimeError):
    pass


@dataclass
class Lake:
    root: Path

    def p(self, *parts: str) -> str:
        return str(self.root / "lake" / Path(*parts))

    @property
    def state_file(self) -> Path:
        return self.root / "lake" / "_state.json"

    def state(self) -> dict:
        if self.state_file.exists():
            return json.loads(self.state_file.read_text())
        return {"last_batch": 0, "dim_version": None}

    def save_state(self, state: dict) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2) + "\n")
        tmp.replace(self.state_file)  # atomic on POSIX: never a half-written watermark


def landed_batches(root: Path) -> list[int]:
    base = root / "landing" / "cdr"
    if not base.exists():
        return []
    return sorted(int(p.name.split("=")[1]) for p in base.glob("batch=*"))


def _read(spark: SparkSession, path: str) -> DataFrame | None:
    return spark.read.parquet(path) if Path(path).exists() else None


def process_batch(
    spark: SparkSession, lake: Lake, batch: int, state: dict, max_reject_rate: float = 0.05
) -> tuple[dict, dict]:
    root = str(lake.root)
    report: dict = {"batch": batch}

    # 1. bronze
    cdr_raw = B.read_landing(spark, root, "cdr", batch)
    cdr_bronze, quarantine = B.cdr_to_bronze(cdr_raw, batch)
    cdc_bronze, cdc_bad = B.cdc_to_bronze(
        B.read_landing(spark, root, "subscriber_cdc", batch), batch
    )
    cdr_bronze.write.mode("overwrite").partitionBy("batch").parquet(lake.p("bronze", "cdr"))
    quarantine.write.mode("overwrite").partitionBy("batch").parquet(
        lake.p("bronze", "cdr_quarantine")
    )
    cdc_bronze.write.mode("overwrite").partitionBy("batch").parquet(
        lake.p("bronze", "subscriber_cdc")
    )
    report["landed_rows"] = cdr_raw.count()
    report["quarantined"] = quarantine.count()
    report["cdc_quarantined"] = cdc_bad.count()

    # 2. SCD2 dimension -> new snapshot; the old one stays readable until commit
    prev = state.get("dim_version")
    dim_prev = _read(spark, lake.p("silver", "dim_subscriber", f"v={prev:03d}")) if prev else None
    cdc = spark.read.parquet(lake.p("bronze", "subscriber_cdc")).filter(F.col("batch") == batch)
    dim, dim_stats = apply_cdc(dim_prev, cdc)
    version = (prev or 0) + 1  # snapshots only ever move forward, even on a reprocess
    dim_path = lake.p("silver", "dim_subscriber", f"v={version:03d}")
    dim.write.mode("overwrite").parquet(dim_path)
    dim = spark.read.parquet(dim_path)
    report["dimension"] = dim_stats

    # 3. silver
    bronze_batch = spark.read.parquet(lake.p("bronze", "cdr")).filter(F.col("batch") == batch)
    valid, rejects = S.validate(bronze_batch, dim)
    existing = _read(spark, lake.p("silver", "cdr"))
    new, dup_stats = S.dedupe(valid, existing)
    # Materialise before overwriting silver: ``new`` is defined by an anti-join
    # against the very files the write below replaces.
    new = new.localCheckpoint(eager=True)
    rejects = rejects.localCheckpoint(eager=True)
    batch_date = (
        spark.read.parquet(lake.p("bronze", "cdr"))
        .filter(F.col("batch") == batch)
        .agg(F.max(F.date_format("start_ts", "yyyy-MM-dd")))
        .first()[0]
    )
    report["late_rows"] = new.filter(F.col("event_date") < batch_date).count()
    touched_dates = sorted(r[0] for r in new.select("event_date").distinct().collect())
    report["silver_rows_written"] = new.count()
    merged = S.merge_partitions(new, existing).localCheckpoint(eager=True)
    if touched_dates:
        merged.write.mode("overwrite").partitionBy("event_date").parquet(lake.p("silver", "cdr"))
    rejects.write.mode("overwrite").partitionBy("batch").parquet(lake.p("silver", "cdr_rejects"))
    report["rejected"] = {
        r["reason"]: r["count"] for r in rejects.groupBy("reason").count().collect()
    }
    report["deduplicated"] = dup_stats
    report["touched_dates"] = touched_dates

    for cached in (new, rejects, merged):  # release the materialised copies
        cached.unpersist()

    # 4. gold, for the touched dates only
    if touched_dates:
        silver_days = spark.read.parquet(lake.p("silver", "cdr")).filter(
            F.col("event_date").isin(touched_dates)
        )
        G.daily_usage(silver_days).write.mode("overwrite").partitionBy("event_date").parquet(
            lake.p("gold", "daily_usage")
        )

    # 5. quality gate
    bad = report["quarantined"] + sum(report["rejected"].values())
    rate = bad / report["landed_rows"] if report["landed_rows"] else 0.0
    report["reject_rate"] = round(rate, 4)
    if rate > max_reject_rate:
        raise QualityGateError(
            f"batch {batch}: {bad}/{report['landed_rows']} rows rejected "
            f"({rate:.1%} > {max_reject_rate:.1%}); watermark not advanced"
        )

    reports = lake.root / "lake" / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    (reports / f"batch={batch:03d}.json").write_text(json.dumps(report, indent=2) + "\n")
    # 6. commit
    return report, {"last_batch": max(batch, state["last_batch"]), "dim_version": version}


def run(
    spark: SparkSession,
    root: str | Path,
    reprocess: list[int] | None = None,
    max_reject_rate: float = 0.05,
) -> list[dict]:
    """Process all batches after the watermark (or exactly ``reprocess``)."""
    lake = Lake(Path(root))
    state = lake.state()
    todo = reprocess or [b for b in landed_batches(lake.root) if b > state["last_batch"]]
    reports = []
    for batch in todo:
        report, state = process_batch(spark, lake, batch, state, max_reject_rate)
        lake.save_state(state)
        reports.append(report)
    return reports
