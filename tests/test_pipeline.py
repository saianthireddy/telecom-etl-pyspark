"""End to end on generated data: every planted fault is caught, exactly."""

import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from telecom_etl.generate import generate
from telecom_etl.pipeline import Lake, QualityGateError, run

ROOT = Path(__file__).resolve().parents[1]


def _silver(spark, root):
    return spark.read.parquet(str(root / "lake" / "silver" / "cdr"))


def _gold(spark, root):
    return spark.read.parquet(str(root / "lake" / "gold" / "daily_usage"))


def test_every_planted_fault_is_caught_exactly(lake):
    _, manifest, reports = lake
    assert [r["batch"] for r in reports] == [1, 2, 3]
    for report in reports:
        f = manifest["batches"][str(report["batch"])]["faults"]
        assert report["quarantined"] == f["bad_timestamp"]
        assert report["rejected"] == {
            "null_msisdn": f["null_msisdn"],
            "negative_duration": f["negative_duration"],
            "bad_event_type": f["bad_event_type"],
            "unknown_msisdn": f["unknown_msisdn"],
        }
        assert report["deduplicated"] == {
            "duplicates_in_batch": f["duplicate_in_batch"],
            "already_loaded": f["replayed"],
        }
        assert report["late_rows"] == f["late"]
        assert report["silver_rows_written"] == f["clean"] + f["late"]


def test_cdc_redeliveries_do_not_create_history(lake):
    _, manifest, reports = lake
    for report in reports:
        assert (
            report["dimension"]["redelivered_ignored"]
            == manifest["batches"][str(report["batch"])]["cdc_replays"]
        )


def test_silver_is_unique_and_complete(spark, lake):
    root, manifest, _ = lake
    silver = _silver(spark, root)
    expected = sum(b["faults"]["clean"] + b["faults"]["late"] for b in manifest["batches"].values())
    assert silver.count() == expected
    assert silver.select("cdr_id").distinct().count() == expected


def test_late_records_land_in_their_own_day_and_gold_agrees(spark, lake):
    root, _, reports = lake
    assert reports[1]["touched_dates"] == ["2026-01-01", "2026-01-02"]
    per_day_silver = {
        r[0]: r[1] for r in _silver(spark, root).groupBy("event_date").count().collect()
    }
    per_day_gold = {
        r[0]: r[1]
        for r in _gold(spark, root).groupBy("event_date").agg(F.sum("cdr_count")).collect()
    }
    assert per_day_gold == per_day_silver
    assert per_day_silver["2026-01-01"] == 1000 + 8  # day 1 clean + its late arrivals


def test_second_run_is_a_noop(spark, lake):
    root, _, _ = lake
    assert run(spark, root) == []


def test_reprocessing_a_batch_changes_nothing(spark, lake):
    root, _, _ = lake

    def snapshot():
        silver = sorted(map(tuple, _silver(spark, root).select("cdr_id", "plan").collect()))
        gold = sorted(map(tuple, _gold(spark, root).collect()))
        return silver, gold

    before = snapshot()
    [report] = run(spark, root, reprocess=[2])
    assert report["silver_rows_written"] == 0
    assert report["dimension"]["applied"] == 0
    assert snapshot() == before
    assert Lake(root).state()["last_batch"] == 3  # the watermark never moves backwards


def test_quality_gate_holds_the_watermark(spark, tmp_path):
    generate(tmp_path, batches=1, subscribers=40, cdrs_per_day=100)
    with pytest.raises(QualityGateError, match="watermark not advanced"):
        run(spark, tmp_path, max_reject_rate=0.01)  # 14 bad of ~124 rows is ~11%
    assert Lake(tmp_path).state()["last_batch"] == 0
    assert not (tmp_path / "lake" / "reports").exists()


def test_reports_are_written(lake):
    root, _, reports = lake
    on_disk = json.loads((root / "lake" / "reports" / "batch=003.json").read_text())
    assert on_disk == reports[2]


def test_readme_numbers_are_current(lake):
    """The README quotes batch 2; fail if the pipeline's output drifts from it."""
    _, _, reports = lake
    r = reports[1]
    row = (
        f"| {r['landed_rows']} | {r['quarantined']} | {sum(r['rejected'].values())} "
        f"| {r['deduplicated']['duplicates_in_batch']} | {r['deduplicated']['already_loaded']} "
        f"| {r['late_rows']} | {r['silver_rows_written']} "
        f"| {r['dimension']['redelivered_ignored']} |"
    )
    assert row in (ROOT / "README.md").read_text(), f"README row out of date: {row}"
