# Telecom ETL on PySpark

[![CI](https://github.com/saianthireddy/telecom-etl-pyspark/actions/workflows/ci.yml/badge.svg)](https://github.com/saianthireddy/telecom-etl-pyspark/actions/workflows/ci.yml)

An incremental PySpark pipeline for telecom call detail records (CDRs). It
handles the problems that make real feeds hard rather than the happy path:

- **CDC into a Type 2 dimension.** Subscriber changes arrive as a change feed
  (insert / update / delete with a log sequence number). They are applied as
  SCD2 history, and redelivered changes are recognised and ignored.
- **Point-in-time joins.** Every call is priced at the plan that was in force
  *when the call happened*. A subscriber who upgrades at 14:00 shows up in
  that day's usage under both plans.
- **Late data.** A record that arrives a day late is written to its own day's
  partition, and that day's aggregates are recomputed.
- **Duplicates and replays.** Records are de-duplicated inside a batch and
  against everything already loaded.
- **Idempotent reruns.** Reprocessing a batch leaves silver and gold with
  exactly the same rows, and a crash mid-batch is simply retried.
- **A data quality gate.** Unreadable rows are quarantined with the raw value
  and every rejected row gets one reason. A batch with too many rejects fails
  and the watermark stays put.

Everything runs locally on Spark 4 with Parquet. There are no cloud services,
no Delta jars and no API keys, so the tests run in CI on every push.

## Results

The generator writes synthetic data and plants known faults in every batch,
recording exactly how many of each kind it planted. The tests then assert the
pipeline catches *exactly* those, no more and no fewer. Here is batch 2 of
the default run (3 days, 200 subscribers, 1,000 calls a day):

| Landed | Quarantined | Rejected | In-batch dupes | Replays dropped | Late rows | Written | CDC redeliveries ignored |
|---|---|---|---|---|---|---|---|
| 1037 | 2 | 12 | 10 | 5 | 8 | 1008 | 4 |

Reading the row:

- **Quarantined (2):** the two planted bad timestamps (`2026-13-45 25:61:00`).
- **Rejected (12):**
  - 3 rows with no subscriber number
  - 3 with negative call durations
  - 2 with an unknown event type (`mms`)
  - 4 from numbers that were never subscribers
- **In-batch dupes (10):** the file delivered ten records twice.
- **Replays dropped (5):** five records from the day before, delivered again.
- **Late rows (8):** yesterday's calls arriving today. They land in yesterday's
  partition, so day 1 ends up with 1,008 rows.
- **Written (1008):** the 1,000 clean calls plus the 8 late ones.
- **CDC redeliveries ignored (4):** subscriber changes the feed had already
  sent once.

A test re-runs the pipeline and fails if this row stops matching.

## How it works

```
landing/            bronze                 silver                     gold
cdr/batch=N   ──►  cast, quarantine  ──►  validate as-of dimension ──► daily usage per
  (CSV)            unreadable rows        dedupe, merge by event date   subscriber, plan,
                                          (late rows → their own day)   region, day
subscriber_cdc/ ─► cast ─────────────►  SCD2 dimension snapshot v=K
batch=N                                  (lsn order, redeliveries dropped)
```

Each run processes every landed batch after the watermark, in order:

1. **Bronze:** read CSVs as strings, then cast. A value that won't cast goes
   to `cdr_quarantine` with its reason and the raw text, instead of becoming
   a silent null.
2. **Dimension:** apply the CDC batch to the SCD2 subscriber table in log
   sequence order. Any change at or below the highest sequence number already
   applied for that subscriber is a redelivery and is dropped.
   - Several changes in one batch keep their intermediate versions.
   - A delete closes the current version and opens nothing.
   - The result is written as a new snapshot (`v=K`), and the old snapshot
     stays readable until commit.
3. **Silver:** join each call to the dimension version valid at its own
   timestamp, then apply the rules, first failing rule wins:
   - `null_msisdn`
   - `bad_event_type`
   - `negative_duration`
   - `negative_bytes`
   - `unknown_msisdn` (never a subscriber, or called after the line closed)

   Then de-duplicate by `cdr_id` and merge into event-date partitions. Dynamic
   partition overwrite replaces only the days the batch touched.
4. **Gold:** recompute daily usage for exactly those days.
5. **Gate:** if quarantined plus rejected rows exceed `--max-reject-rate`
   (default 5%), raise. Nothing is committed.
6. **Commit:** write the batch report and atomically replace `_state.json`,
   which holds the watermark and the current dimension snapshot.

### Why reruns are safe

Every write is keyed:

- bronze by batch
- silver and gold by event date
- the dimension by a new snapshot number

Rerunning batch N therefore replaces exactly what batch N wrote. Records it
already loaded are dropped as duplicates, and its CDC changes are all
redeliveries. `test_reprocessing_a_batch_changes_nothing` checks that the
silver and gold tables are identical before and after a reprocess, and that
the watermark does not move backwards.

## Things the tests caught while building this

- **Reading and overwriting the same table in one job.** New rows are found
  by anti-joining the batch against silver, then silver is overwritten. Spark
  evaluates lazily, so the anti-join re-read files the overwrite had just
  deleted (`FileNotFoundException`). The batch is now materialised
  (`localCheckpoint`) before the write, and released after it.
- **Time zones in tests.** Hand-built test rows use Python `datetime`s, which
  PySpark converts using the *machine's* local time zone. On a machine set to
  US Central, a 12:00 plan change became 18:00 UTC, and calls were priced at
  the wrong plan. The pipeline only parses strings in UTC, so this was a test
  artefact, but it is exactly the bug that ships when tests run in one zone
  and production in another. The tests now pin `TZ=UTC`.
- **Partition type inference.** Read back, `event_date=2026-01-01` came back
  as a `date` in one place and was compared with a string in another.
  Inference is now off, so partition columns stay the strings they were
  written as.

## Run it

Needs Java 17+ and Python 3.10+.

```bash
pip install -e ".[dev]"
python -m telecom_etl generate --root data --batches 3
python -m telecom_etl run --root data        # processes batches 1..3
python -m telecom_etl run --root data        # "nothing to do": watermark is at 3
python -m telecom_etl run --root data --reprocess 2
pytest -q                                    # about a minute
```

Per-batch reports are written to `data/lake/reports/batch=NNN.json`.

## Layout

```
src/telecom_etl/
  generate.py   synthetic CDRs + CDC with planted faults and a manifest of them
  bronze.py     typed landing, quarantine with reasons
  scd2.py       CDC → SCD2 (lsn ordering, redelivery, deletes) and the as-of join
  silver.py     rules, de-duplication, event-date merge
  gold.py       daily usage at the plan in force at call time
  pipeline.py   watermark, snapshots, quality gate, reports
  schemas.py    column definitions per layer
  spark.py      one SparkSession config for the CLI and the tests
tests/          end-to-end on generated data + SCD2 cases small enough to check by hand
```

## Limitations

- Parquet with dynamic partition overwrite, not a table format. Delta Lake or
  Iceberg would give real `MERGE`, time travel and concurrent writers. The
  logic here is written so it maps onto a `MERGE` directly.
- Single writer: `_state.json` assumes one pipeline run at a time.
- Each dimension snapshot is a full copy. That's fine for a subscriber table
  of this size, but a large one would want a table format's incremental
  merge.
- The data is synthetic and small. These numbers show correctness, not
  throughput.

## License

MIT
