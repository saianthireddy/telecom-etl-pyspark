"""Schemas for every layer. Landing files are read as strings on purpose:
casting happens in bronze, where a value that will not parse is quarantined
with a reason instead of silently becoming null."""

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CDR_RAW = StructType(
    [
        StructField(c, StringType())
        for c in ("cdr_id", "msisdn", "event_type", "start_ts", "duration_s", "bytes", "cell_id")
    ]
)

CDC_RAW = StructType(
    [
        StructField(c, StringType())
        for c in ("lsn", "op", "msisdn", "plan", "region", "status", "change_ts")
    ]
)

CDR_BRONZE = StructType(
    [
        StructField("cdr_id", StringType()),
        StructField("msisdn", StringType()),
        StructField("event_type", StringType()),
        StructField("start_ts", TimestampType()),
        StructField("duration_s", IntegerType()),
        StructField("bytes", LongType()),
        StructField("cell_id", StringType()),
        StructField("batch", IntegerType()),
    ]
)

EVENT_TYPES = ("voice", "sms", "data")

DIM_SUBSCRIBER = StructType(
    [
        StructField("msisdn", StringType()),
        StructField("plan", StringType()),
        StructField("region", StringType()),
        StructField("status", StringType()),
        StructField("valid_from", TimestampType()),
        StructField("valid_to", TimestampType()),
        StructField("is_current", BooleanType()),
        StructField("lsn", LongType()),
    ]
)

GOLD_DAILY = StructType(
    [
        StructField("event_date", StringType()),
        StructField("msisdn", StringType()),
        StructField("plan", StringType()),
        StructField("region", StringType()),
        StructField("voice_minutes", DoubleType()),
        StructField("sms_count", LongType()),
        StructField("data_mb", DoubleType()),
        StructField("cdr_count", LongType()),
    ]
)
