"""Bronze: land raw files as typed rows, without judging them yet.

Every column is read as a string and cast here. A value that will not cast
(a timestamp like ``2026-13-45``) goes to quarantine with the reason and the
original text, rather than becoming a silent null that a later rule might
misreport. Business rules (negative durations, unknown numbers) are silver's
job: bronze only answers "is this row readable?".
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .schemas import CDC_RAW, CDR_RAW


def read_landing(spark: SparkSession, root: str, feed: str, batch: int) -> DataFrame:
    schema = CDR_RAW if feed == "cdr" else CDC_RAW
    path = f"{root}/landing/{feed}/batch={batch:03d}"
    return spark.read.csv(path, header=True, schema=schema, mode="PERMISSIVE")


def cdr_to_bronze(raw: DataFrame, batch: int) -> tuple[DataFrame, DataFrame]:
    """Returns (bronze rows, quarantined rows with a ``reason``)."""
    typed = raw.select(
        F.col("cdr_id"),
        F.when(F.trim("msisdn") == "", None).otherwise(F.trim("msisdn")).alias("msisdn"),
        F.lower(F.trim("event_type")).alias("event_type"),
        F.try_to_timestamp(F.col("start_ts"), F.lit("yyyy-MM-dd HH:mm:ss")).alias("start_ts"),
        F.expr("try_cast(duration_s as int)").alias("duration_s"),
        F.expr("try_cast(bytes as bigint)").alias("bytes"),
        F.col("cell_id"),
        F.lit(batch).alias("batch"),
        F.col("start_ts").alias("_raw_start_ts"),
        F.col("duration_s").alias("_raw_duration_s"),
        F.col("bytes").alias("_raw_bytes"),
    )
    reason = (
        F.when(F.col("cdr_id").isNull(), "missing cdr_id")
        .when(
            F.col("start_ts").isNull(),
            F.concat(F.lit("unparseable start_ts: "), F.coalesce("_raw_start_ts", F.lit("null"))),
        )
        .when(
            F.col("duration_s").isNull() & F.col("_raw_duration_s").isNotNull(),
            "unparseable duration_s",
        )
        .when(F.col("bytes").isNull() & F.col("_raw_bytes").isNotNull(), "unparseable bytes")
    )
    tagged = typed.withColumn("reason", reason)
    raw_cols = ["_raw_start_ts", "_raw_duration_s", "_raw_bytes"]
    good = tagged.filter(F.col("reason").isNull()).drop("reason", *raw_cols)
    quarantined = tagged.filter(F.col("reason").isNotNull()).select(
        "cdr_id", "msisdn", "batch", "reason", F.col("_raw_start_ts").alias("raw_start_ts")
    )
    return good, quarantined


def cdc_to_bronze(raw: DataFrame, batch: int) -> tuple[DataFrame, DataFrame]:
    typed = raw.select(
        F.expr("try_cast(lsn as bigint)").alias("lsn"),
        F.upper(F.trim("op")).alias("op"),
        F.trim("msisdn").alias("msisdn"),
        "plan",
        "region",
        "status",
        F.try_to_timestamp(F.col("change_ts"), F.lit("yyyy-MM-dd HH:mm:ss")).alias("change_ts"),
        F.lit(batch).alias("batch"),
    )
    reason = (
        F.when(F.col("lsn").isNull(), "missing or bad lsn")
        .when(~F.col("op").isin("I", "U", "D"), "unknown op")
        .when(F.col("msisdn").isNull(), "missing msisdn")
        .when(F.col("change_ts").isNull(), "bad change_ts")
    )
    tagged = typed.withColumn("reason", reason)
    return (
        tagged.filter(F.col("reason").isNull()).drop("reason"),
        tagged.filter(F.col("reason").isNotNull()),
    )
