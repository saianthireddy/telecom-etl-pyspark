"""Gold: daily usage per subscriber, at the plan and region in force at call time.

A subscriber who upgrades plan at 14:00 contributes two rows that day: the
morning's usage under the old plan and the afternoon's under the new one.
That comes for free from silver, where each CDR already carries the
dimension version valid at its own timestamp.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def daily_usage(silver: DataFrame) -> DataFrame:
    return silver.groupBy("event_date", "msisdn", "plan", "region").agg(
        F.round(
            F.sum(
                F.when(F.col("event_type") == "voice", F.col("duration_s") / 60.0).otherwise(0.0)
            ),
            2,
        ).alias("voice_minutes"),
        F.sum(F.when(F.col("event_type") == "sms", 1).otherwise(0)).cast("long").alias("sms_count"),
        F.round(
            F.sum(F.when(F.col("event_type") == "data", F.col("bytes") / 1e6).otherwise(0.0)), 3
        ).alias("data_mb"),
        F.count("*").alias("cdr_count"),
    )
