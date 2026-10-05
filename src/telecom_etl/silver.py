"""Silver: validated, de-duplicated CDRs, partitioned by event date.

Rules (first failing rule wins, so each rejected row has one reason):
  null_msisdn         no subscriber number
  bad_event_type      not voice / sms / data
  negative_duration   duration_s < 0
  negative_bytes      bytes < 0
  unknown_msisdn      no subscriber version was valid at the event time
                      (never existed, or the line was closed before the call)

De-duplication is by ``cdr_id`` at two levels:
  * inside the batch (the same file delivering a record twice)
  * against silver itself (a record replayed from an earlier batch)

A late CDR, one whose event date is before the batch date, is not special:
it is written to its own event-date partition. Because writes overwrite only
the partitions they touch, the old day is rebuilt from its existing rows plus
the late ones, and gold is recomputed for exactly those dates.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .scd2 import as_of_join
from .schemas import EVENT_TYPES


def validate(bronze: DataFrame, dim: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (valid rows enriched with plan/region as of event time, rejects)."""
    enriched = as_of_join(bronze, dim, "start_ts")
    reason = (
        F.when(F.col("msisdn").isNull(), "null_msisdn")
        .when(~F.col("event_type").isin(*EVENT_TYPES), "bad_event_type")
        .when(F.col("duration_s") < 0, "negative_duration")
        .when(F.col("bytes") < 0, "negative_bytes")
        .when(F.col("plan").isNull(), "unknown_msisdn")
    )
    tagged = enriched.withColumn("reason", reason)
    valid = tagged.filter(F.col("reason").isNull()).drop("reason", "valid_from", "valid_to")
    rejects = tagged.filter(F.col("reason").isNotNull()).select(
        "cdr_id", "msisdn", "event_type", "start_ts", "batch", "reason"
    )
    return valid.withColumn("event_date", F.date_format("start_ts", "yyyy-MM-dd")), rejects


def dedupe(valid: DataFrame, existing: DataFrame | None) -> tuple[DataFrame, dict]:
    """Drop in-batch duplicates and records already in silver."""
    in_batch = valid.count()
    unique = valid.dropDuplicates(["cdr_id"])
    after_batch = unique.count()
    if existing is not None:
        unique = unique.join(existing.select("cdr_id"), "cdr_id", "left_anti")
    after_existing = unique.count()
    return unique, {
        "duplicates_in_batch": in_batch - after_batch,
        "already_loaded": after_batch - after_existing,
    }


def merge_partitions(new: DataFrame, existing: DataFrame | None) -> DataFrame:
    """Rows for every event date the batch touches: what was there plus what is new.
    Written with dynamic partition overwrite, only those dates are replaced."""
    if existing is None:
        return new
    dates = new.select("event_date").distinct()
    kept = existing.join(dates, "event_date", "left_semi")
    return kept.unionByName(new.select(existing.columns))
