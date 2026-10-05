"""Apply a subscriber CDC batch to a Type 2 slowly changing dimension.

Each subscriber keeps one row per version, with ``valid_from`` / ``valid_to``
and ``is_current``. CDC rows carry a log sequence number (``lsn``), and order
is decided by lsn, not by arrival.

Rules:
  * A change whose lsn is not greater than the highest lsn already applied for
    that subscriber is a redelivery and is dropped. That makes re-running a
    batch, or a CDC source replaying after a restart, a no-op.
  * I/U open a new version and close the previous one at the change time.
  * D closes the current version and opens nothing.
  * Several changes for one subscriber in one batch are applied in lsn order,
    so intermediate versions are kept, not collapsed.

Only the rows of subscribers touched by the batch are rewritten; closed history
for everyone else passes through unchanged.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

VERSION_COLS = ["msisdn", "plan", "region", "status", "valid_from", "valid_to", "is_current", "lsn"]


def apply_cdc(dim: DataFrame | None, changes: DataFrame) -> tuple[DataFrame, dict]:
    """Return (new dimension, stats). ``dim`` may be None for the first load."""
    spark = changes.sparkSession
    if dim is None:
        dim = spark.createDataFrame(
            [],
            "msisdn string, plan string, region string, "
            "status string, valid_from timestamp, "
            "valid_to timestamp, is_current boolean, lsn bigint",
        )

    # exact duplicates inside the batch, then anything at or below the applied lsn
    changes = changes.dropDuplicates(["msisdn", "lsn"])
    applied = dim.groupBy("msisdn").agg(F.max("lsn").alias("applied_lsn"))
    fresh = (
        changes.join(applied, "msisdn", "left")
        .filter(F.col("applied_lsn").isNull() | (F.col("lsn") > F.col("applied_lsn")))
        .drop("applied_lsn")
    )
    stats = {"cdc_rows": changes.count(), "applied": fresh.count()}
    stats["redelivered_ignored"] = stats["cdc_rows"] - stats["applied"]

    touched = fresh.select("msisdn").distinct()
    untouched = dim.join(touched, "msisdn", "left_anti")
    affected = dim.join(touched, "msisdn", "left_semi")

    closed_history = affected.filter(~F.col("is_current"))
    # the open version re-enters the timeline as an event, so a new change can close it
    open_events = affected.filter(F.col("is_current")).select(
        "msisdn",
        "plan",
        "region",
        "status",
        F.col("valid_from").alias("ts"),
        "lsn",
        F.lit("C").alias("op"),
    )
    new_events = fresh.select(
        "msisdn", "plan", "region", "status", F.col("change_ts").alias("ts"), "lsn", "op"
    )
    events = open_events.unionByName(new_events)

    w = Window.partitionBy("msisdn").orderBy("lsn")
    timeline = events.withColumn("valid_to", F.lead("ts").over(w)).withColumn(
        "is_last", F.lead("lsn").over(w).isNull()
    )
    versions = timeline.filter(F.col("op") != "D").select(
        "msisdn",
        "plan",
        "region",
        "status",
        F.col("ts").alias("valid_from"),
        "valid_to",
        F.col("is_last").alias("is_current"),
        "lsn",
    )
    # A delete leaves no open version, but its lsn must still count as applied,
    # or a redelivered delete would look new. Keep it as a zero-length marker.
    deletes = timeline.filter(F.col("op") == "D").select(
        "msisdn",
        "plan",
        "region",
        F.lit("deleted").alias("status"),
        F.col("ts").alias("valid_from"),
        F.col("ts").alias("valid_to"),
        F.lit(False).alias("is_current"),
        "lsn",
    )
    result = (
        untouched.select(VERSION_COLS)
        .unionByName(closed_history.select(VERSION_COLS))
        .unionByName(versions.select(VERSION_COLS))
        .unionByName(deletes.select(VERSION_COLS))
    )
    stats["subscribers_changed"] = touched.count()
    return result, stats


def as_of_join(facts: DataFrame, dim: DataFrame, ts_col: str) -> DataFrame:
    """Attach the dimension version that was valid when each fact happened.
    Facts with no valid version (unknown number, or after a delete) get nulls."""
    d = dim.filter(
        F.col("valid_from") < F.coalesce("valid_to", F.lit("9999-12-31").cast("timestamp"))
    )
    cond = (
        (facts["msisdn"] == d["msisdn"])
        & (facts[ts_col] >= d["valid_from"])
        & (d["valid_to"].isNull() | (facts[ts_col] < d["valid_to"]))
    )
    return facts.join(
        d.select("msisdn", "plan", "region", "valid_from", "valid_to"), cond, "left"
    ).drop(d["msisdn"])
