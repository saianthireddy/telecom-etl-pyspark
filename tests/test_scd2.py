"""SCD2 and point-in-time join, on hand-built rows small enough to reason about."""

from datetime import datetime

from pyspark.sql import functions as F

from telecom_etl.bronze import cdr_to_bronze
from telecom_etl.gold import daily_usage
from telecom_etl.scd2 import apply_cdc
from telecom_etl.silver import validate

CDC = (
    "lsn bigint, op string, msisdn string, plan string, region string, status string, "
    "change_ts timestamp, batch int"
)


def t(h, d=1):
    return datetime(2026, 1, d, h)


def _cdc(spark, rows):
    return spark.createDataFrame(rows, CDC)


def apply(dim, changes):
    """The pipeline writes each snapshot and reads it back; do the same here,
    or chained applies build an ever-growing query plan."""
    result, stats = apply_cdc(dim, changes)
    return result.localCheckpoint(eager=True), stats


def _versions(dim, msisdn="A"):
    return [
        (r.plan, r.valid_from, r.valid_to, r.is_current)
        for r in dim.filter(F.col("msisdn") == msisdn)
        .filter(F.col("status") != "deleted")
        .orderBy("lsn")
        .collect()
    ]


def test_changes_in_one_batch_keep_intermediate_versions(spark):
    dim, stats = apply(
        None,
        _cdc(
            spark,
            [
                (1, "I", "A", "basic", "north", "active", t(0), 1),
                (3, "U", "A", "unlimited", "north", "active", t(18), 1),
                (2, "U", "A", "plus", "north", "active", t(9), 1),  # arrives out of order
            ],
        ),
    )
    assert _versions(dim) == [
        ("basic", t(0), t(9), False),
        ("plus", t(9), t(18), False),
        ("unlimited", t(18), None, True),
    ]
    assert stats["applied"] == 3


def test_next_batch_closes_the_open_version_and_redelivery_is_ignored(spark):
    dim, _ = apply(
        None,
        _cdc(
            spark,
            [
                (1, "I", "A", "basic", "north", "active", t(0), 1),
                (2, "I", "B", "plus", "south", "active", t(0), 1),
            ],
        ),
    )
    dim, stats = apply(
        dim,
        _cdc(
            spark,
            [
                (1, "I", "A", "basic", "north", "active", t(0), 2),  # redelivered
                (3, "U", "A", "unlimited", "north", "active", t(12, 2), 2),
            ],
        ),
    )
    assert stats == {
        "cdc_rows": 2,
        "applied": 1,
        "redelivered_ignored": 1,
        "subscribers_changed": 1,
    }
    assert _versions(dim) == [("basic", t(0), t(12, 2), False), ("unlimited", t(12, 2), None, True)]
    assert _versions(dim, "B") == [("plus", t(0), None, True)]  # untouched


def test_delete_closes_without_opening_and_cannot_be_replayed(spark):
    dim, _ = apply(None, _cdc(spark, [(1, "I", "A", "basic", "north", "active", t(0), 1)]))
    deletion = _cdc(spark, [(2, "D", "A", "basic", "north", "closed", t(15), 2)])
    dim, _ = apply(dim, deletion)
    assert _versions(dim) == [("basic", t(0), t(15), False)]
    again, stats = apply(dim, deletion)
    assert stats["redelivered_ignored"] == 1
    assert sorted(map(tuple, again.collect())) == sorted(map(tuple, dim.collect()))


def test_calls_get_the_plan_in_force_at_call_time(spark):
    dim, _ = apply(
        None,
        _cdc(
            spark,
            [
                (1, "I", "A", "basic", "north", "active", t(0), 1),
                (2, "U", "A", "plus", "north", "active", t(12), 1),
                (3, "D", "A", "plus", "north", "closed", t(20), 1),
            ],
        ),
    )
    raw = spark.createDataFrame(
        [
            ("c1", "A", "voice", "2026-01-01 08:00:00", "120", "0", "X"),
            ("c2", "A", "voice", "2026-01-01 13:00:00", "60", "0", "X"),
            ("c3", "A", "sms", "2026-01-01 21:00:00", "0", "0", "X"),  # after the line closed
            ("c4", "Z", "sms", "2026-01-01 09:00:00", "0", "0", "X"),  # never a subscriber
        ],
        "cdr_id string, msisdn string, event_type string, start_ts string, "
        "duration_s string, bytes string, cell_id string",
    )
    bronze, _ = cdr_to_bronze(raw, 1)
    valid, rejects = validate(bronze, dim)
    assert {r.cdr_id: r.plan for r in valid.collect()} == {"c1": "basic", "c2": "plus"}
    assert {r.cdr_id: r.reason for r in rejects.collect()} == {
        "c3": "unknown_msisdn",
        "c4": "unknown_msisdn",
    }

    gold = {(r.plan, r.voice_minutes) for r in daily_usage(valid).collect()}
    assert gold == {("basic", 2.0), ("plus", 1.0)}  # one day, split at the upgrade


def test_bronze_quarantines_unreadable_rows_with_the_raw_value(spark):
    raw = spark.createDataFrame(
        [
            ("c1", "A", "voice", "2026-13-45 25:61:00", "10", "0", "X"),
            ("c2", "A", "voice", "2026-01-01 10:00:00", "ten", "0", "X"),
            ("c3", "A", "voice", "2026-01-01 10:00:00", "10", "0", "X"),
        ],
        "cdr_id string, msisdn string, event_type string, start_ts string, "
        "duration_s string, bytes string, cell_id string",
    )
    good, quarantined = cdr_to_bronze(raw, 1)
    assert [r.cdr_id for r in good.collect()] == ["c3"]
    reasons = {r.cdr_id: r.reason for r in quarantined.collect()}
    assert reasons == {
        "c1": "unparseable start_ts: 2026-13-45 25:61:00",
        "c2": "unparseable duration_s",
    }
