"""Deterministic synthetic landing data: CDRs plus a subscriber CDC feed.

Each batch is one day. Into every CDR batch the generator plants known faults
and records how many of each it planted (``manifest.json``), so tests can
assert the pipeline catches exactly those, no more and no fewer:

  duplicate_in_batch   the same CDR delivered twice in one file
  replayed             a CDR from the previous batch delivered again
  late                 a valid CDR for the previous day arriving a day late
  null_msisdn          no subscriber number
  negative_duration    duration_s < 0
  bad_event_type       not voice / sms / data
  unknown_msisdn       a number that was never a subscriber
  bad_timestamp        start_ts that does not parse (quarantined in bronze)

The CDC feed inserts every subscriber in batch 1, then each later batch has
plan changes, a few deletes, and replays of already-applied changes (a CDC
source redelivering after a restart), which must not create new history.

All data is synthetic; no real subscriber data is involved.
"""

from __future__ import annotations

import csv
import json
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

PLANS = ("prepaid_basic", "postpaid_plus", "unlimited")
REGIONS = ("north", "south", "east", "west")
DAY0 = datetime(2026, 1, 1)
TS = "%Y-%m-%d %H:%M:%S"

CDR_COLUMNS = ["cdr_id", "msisdn", "event_type", "start_ts", "duration_s", "bytes", "cell_id"]
CDC_COLUMNS = ["lsn", "op", "msisdn", "plan", "region", "status", "change_ts"]

FAULTS = {
    "duplicate_in_batch": 10,
    "replayed": 5,
    "late": 8,
    "null_msisdn": 3,
    "negative_duration": 3,
    "bad_event_type": 2,
    "unknown_msisdn": 4,
    "bad_timestamp": 2,
}


@dataclass
class Subscriber:
    msisdn: str
    plan: str
    region: str
    deleted_at: datetime | None = None
    changed_at: datetime | None = None


@dataclass
class World:
    rng: random.Random
    subscribers: dict[str, Subscriber] = field(default_factory=dict)
    lsn: int = 0
    cdr_seq: int = 0
    applied_cdc: list[dict] = field(default_factory=list)
    last_batch_cdrs: list[dict] = field(default_factory=list)

    def next_lsn(self) -> int:
        self.lsn += 1
        return self.lsn

    def next_cdr_id(self) -> str:
        self.cdr_seq += 1
        return f"C{self.cdr_seq:08d}"


def _day(batch: int) -> datetime:
    return DAY0 + timedelta(days=batch - 1)


def _cdr(world: World, msisdn: str, when: datetime) -> dict:
    rng = world.rng
    event_type = rng.choices(("voice", "sms", "data"), weights=(5, 3, 4))[0]
    duration = rng.randint(5, 1800) if event_type == "voice" else 0
    nbytes = rng.randint(10_000, 50_000_000) if event_type == "data" else 0
    return {
        "cdr_id": world.next_cdr_id(),
        "msisdn": msisdn,
        "event_type": event_type,
        "start_ts": when.strftime(TS),
        "duration_s": str(duration),
        "bytes": str(nbytes),
        "cell_id": f"CELL{rng.randint(1, 300):04d}",
    }


def _active(sub: Subscriber, when: datetime) -> bool:
    return sub.deleted_at is None or when < sub.deleted_at


def _cdc_batch(world: World, batch: int) -> list[dict]:
    rng, day = world.rng, _day(batch)
    rows = []
    if batch == 1:
        for sub in world.subscribers.values():
            rows.append(
                {
                    "lsn": world.next_lsn(),
                    "op": "I",
                    "msisdn": sub.msisdn,
                    "plan": sub.plan,
                    "region": sub.region,
                    "status": "active",
                    "change_ts": day.strftime(TS),
                }
            )
    else:
        live = [s for s in world.subscribers.values() if s.deleted_at is None]
        for sub in rng.sample(live, 12):  # plan changes at a time during the day
            when = day + timedelta(seconds=rng.randint(3600, 79_200))
            sub.plan = rng.choice([p for p in PLANS if p != sub.plan])
            sub.changed_at = when
            rows.append(
                {
                    "lsn": world.next_lsn(),
                    "op": "U",
                    "msisdn": sub.msisdn,
                    "plan": sub.plan,
                    "region": sub.region,
                    "status": "active",
                    "change_ts": when.strftime(TS),
                }
            )
        live = [s for s in live if s.changed_at is None or s.changed_at < day]
        for sub in rng.sample(live, 3):
            when = day + timedelta(seconds=rng.randint(3600, 79_200))
            sub.deleted_at = when
            rows.append(
                {
                    "lsn": world.next_lsn(),
                    "op": "D",
                    "msisdn": sub.msisdn,
                    "plan": sub.plan,
                    "region": sub.region,
                    "status": "closed",
                    "change_ts": when.strftime(TS),
                }
            )
        rows.extend(dict(r) for r in rng.sample(world.applied_cdc, 4))  # redelivery
    world.applied_cdc.extend(r for r in rows if r not in world.applied_cdc)
    return rows


def _cdr_batch(world: World, batch: int, per_day: int) -> tuple[list[dict], dict]:
    rng, day = world.rng, _day(batch)
    subs = list(world.subscribers.values())
    clean = []
    while len(clean) < per_day:
        sub = rng.choice(subs)
        when = day + timedelta(seconds=rng.randint(0, 86_399))
        if _active(sub, when):
            clean.append(_cdr(world, sub.msisdn, when))
    faults = {k: 0 for k in FAULTS}
    rows = list(clean)

    def bad(mutate, kind):
        sub = rng.choice([s for s in subs if s.deleted_at is None])
        row = _cdr(world, sub.msisdn, day + timedelta(seconds=rng.randint(0, 86_399)))
        mutate(row)
        rows.append(row)
        faults[kind] += 1

    for _ in range(FAULTS["null_msisdn"]):
        bad(lambda r: r.update(msisdn=""), "null_msisdn")
    for _ in range(FAULTS["negative_duration"]):
        bad(
            lambda r: r.update(event_type="voice", duration_s=str(-rng.randint(1, 60))),
            "negative_duration",
        )
    for _ in range(FAULTS["bad_event_type"]):
        bad(lambda r: r.update(event_type="mms"), "bad_event_type")
    for i in range(FAULTS["unknown_msisdn"]):
        bad(lambda r, i=i: r.update(msisdn=f"1999{batch:02d}{i:04d}"), "unknown_msisdn")
    for _ in range(FAULTS["bad_timestamp"]):
        bad(lambda r: r.update(start_ts="2026-13-45 25:61:00"), "bad_timestamp")

    if batch > 1:
        stable = [s for s in subs if s.deleted_at is None and s.changed_at is None]
        for _ in range(FAULTS["late"]):
            when = day - timedelta(seconds=rng.randint(1, 86_399))
            rows.append(_cdr(world, rng.choice(stable).msisdn, when))
            faults["late"] += 1
        for row in rng.sample(world.last_batch_cdrs, FAULTS["replayed"]):
            rows.append(dict(row))
            faults["replayed"] += 1
    for row in rng.sample(clean, FAULTS["duplicate_in_batch"]):
        rows.append(dict(row))
        faults["duplicate_in_batch"] += 1

    world.last_batch_cdrs = clean
    rng.shuffle(rows)
    faults["clean"] = len(clean)
    return rows, faults


def _write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def generate(
    root: str | Path,
    batches: int = 3,
    subscribers: int = 200,
    cdrs_per_day: int = 1000,
    seed: int = 7,
) -> dict:
    """Write ``batches`` days of landing data under ``root/landing`` and
    return the fault manifest (also written to ``root/landing/manifest.json``)."""
    root = Path(root)
    rng = random.Random(seed)
    world = World(rng)
    for i in range(subscribers):
        msisdn = f"1555{i:06d}"
        world.subscribers[msisdn] = Subscriber(msisdn, rng.choice(PLANS), rng.choice(REGIONS))

    manifest = {"seed": seed, "subscribers": subscribers, "batches": {}}
    for batch in range(1, batches + 1):
        cdc = _cdc_batch(world, batch)
        cdrs, faults = _cdr_batch(world, batch, cdrs_per_day)
        _write_csv(
            root / "landing" / "subscriber_cdc" / f"batch={batch:03d}" / "part-0.csv",
            CDC_COLUMNS,
            cdc,
        )
        _write_csv(
            root / "landing" / "cdr" / f"batch={batch:03d}" / "part-0.csv", CDR_COLUMNS, cdrs
        )
        manifest["batches"][str(batch)] = {
            "cdr_rows": len(cdrs),
            "cdc_rows": len(cdc),
            "cdc_replays": 4 if batch > 1 else 0,
            "faults": faults,
        }
    (root / "landing" / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest
