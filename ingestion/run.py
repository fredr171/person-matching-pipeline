"""Run ingestion load by load: CDC events -> current state (DESIGN.md section 4).

For each load N this reads ``data/loads/load_NNNN/events.parquet`` and the
state after load N-1, and writes to ``data/ingested/load_NNNN/``:

``state.parquet``
    Every key ever seen, including tombstones. Input to load N+1.
``current_records.parquet``
    Live records only (one row per live source record). Input to matching.
``rejects.parquet``
    Events rejected by validation in this load, with reason codes.
``metrics.json``
    Counts of events read, rejected, deduplicated, ignored and applied.

Re-running any load from the previous load's state reproduces its outputs
byte for byte, because rows are written sorted by key.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.apply import ApplyStats, Key, RecordState, apply_events
from ingestion.validate import validate_event
from pipeline import paths

STATE_SCHEMA = pa.schema(
    [
        ("source_system", pa.string()),
        ("source_record_id", pa.string()),
        ("record_life", pa.int64()),
        ("is_deleted", pa.bool_()),
        ("sequence_no", pa.int64()),
        ("event_id", pa.string()),
        ("event_ts", pa.timestamp("us")),
        ("life_started_load", pa.int64()),
        ("last_changed_load", pa.int64()),
        ("first_name", pa.string()),
        ("last_name", pa.string()),
        ("dob", pa.string()),
        ("ssn", pa.string()),
        ("gender", pa.string()),
    ]
)
CURRENT_SCHEMA = pa.schema([f for f in STATE_SCHEMA if f.name != "is_deleted"])
REJECT_SCHEMA = pa.schema(
    [
        ("load_id", pa.int64()),
        ("event_id", pa.string()),
        ("source_system", pa.string()),
        ("source_record_id", pa.string()),
        ("sequence_no", pa.int64()),
        ("op", pa.string()),
        ("reason_code", pa.string()),
        ("detail", pa.string()),
        ("first_name", pa.string()),
        ("last_name", pa.string()),
        ("dob", pa.string()),
        ("ssn", pa.string()),
        ("gender", pa.string()),
    ]
)
REJECT_REASONS = ("REJECT_MISSING_KEY", "REJECT_BAD_OP", "REJECT_COLUMN_SHIFT", "REJECT_ENCODING")


@dataclass(frozen=True)
class LoadMetrics:
    load_id: int
    events_read: int
    rejected: dict[str, int]
    duplicates_dropped: int
    stale_ignored: int
    applied: dict[str, int]
    new_lives: int
    live_records: int
    tombstones: int


# --------------------------------------------------------------------------- #
# Reading and writing
# --------------------------------------------------------------------------- #


def read_events(load_id: int, data_dir: Path) -> list[dict]:
    path = paths.load_dir(load_id, data_dir) / "events.parquet"
    if not path.is_file():
        raise FileNotFoundError(f"No events for load {load_id}: {path}")
    return pq.read_table(path).to_pylist()


def read_state(load_id: int, data_dir: Path) -> dict[Key, RecordState]:
    """State after ``load_id``; an empty state before load 0."""
    if load_id < 0:
        return {}
    path = paths.ingested_dir(load_id, data_dir) / "state.parquet"
    if not path.is_file():
        raise FileNotFoundError(
            f"Load {load_id} has not been ingested ({path}); ingest earlier loads first"
        )
    state = {}
    for row in pq.read_table(path).to_pylist():
        record = RecordState(**row)
        state[record.key] = record
    return state


def _write_table(rows: list[dict], schema: pa.Schema, path: Path) -> None:
    columns = {name: [row[name] for row in rows] for name in schema.names}
    pq.write_table(pa.table(columns, schema=schema), path, compression="snappy")


def _write_outputs(
    load_id: int,
    data_dir: Path,
    state: dict[Key, RecordState],
    rejects: list[dict],
    metrics: LoadMetrics,
) -> None:
    out = paths.ingested_dir(load_id, data_dir)
    out.mkdir(parents=True, exist_ok=True)
    rows = [asdict(state[key]) for key in sorted(state)]
    _write_table(rows, STATE_SCHEMA, out / "state.parquet")
    _write_table(
        [r for r in rows if not r["is_deleted"]], CURRENT_SCHEMA, out / "current_records.parquet"
    )
    _write_table(rejects, REJECT_SCHEMA, out / "rejects.parquet")
    (out / "metrics.json").write_text(
        json.dumps(asdict(metrics), indent=2) + "\n", encoding="utf-8", newline="\n"
    )


# --------------------------------------------------------------------------- #
# Ingestion
# --------------------------------------------------------------------------- #


def ingest_events(
    state: dict[Key, RecordState], events: list[dict], load_id: int
) -> tuple[list[dict], ApplyStats]:
    """Validate and apply one load's events to ``state`` in place; return the rejects."""
    valid, rejects = [], []
    for event in events:
        rejection = validate_event(event)
        if rejection is None:
            valid.append(event)
            continue
        rejects.append(
            {
                **{name: event.get(name) for name in REJECT_SCHEMA.names},
                "load_id": load_id,
                "reason_code": rejection.reason,
                "detail": rejection.detail,
            }
        )
    stats = apply_events(state, valid, load_id)
    return rejects, stats


def ingest_load(
    load_id: int, data_dir: Path = paths.DEFAULT_DATA_DIR, state: dict | None = None
) -> tuple[dict[Key, RecordState], LoadMetrics]:
    """Ingest one load on top of the state after the previous load.

    Pass ``state`` to continue from an in-memory state; otherwise the previous
    load's ``state.parquet`` is read.
    """
    state = dict(read_state(load_id - 1, data_dir) if state is None else state)
    events = read_events(load_id, data_dir)
    rejects, stats = ingest_events(state, events, load_id)

    rejected = dict.fromkeys(REJECT_REASONS, 0)
    for reject in rejects:
        rejected[reject["reason_code"]] += 1
    live = sum(1 for record in state.values() if not record.is_deleted)
    metrics = LoadMetrics(
        load_id=load_id,
        events_read=len(events),
        rejected=rejected,
        duplicates_dropped=stats.duplicates_dropped,
        stale_ignored=stats.stale_ignored,
        applied=stats.applied,
        new_lives=stats.new_lives,
        live_records=live,
        tombstones=len(state) - live,
    )
    _write_outputs(load_id, data_dir, state, rejects, metrics)
    return state, metrics


def available_loads(data_dir: Path) -> list[int]:
    loads_root = data_dir / "loads"
    return sorted(int(p.name.split("_")[1]) for p in loads_root.glob("load_*") if p.is_dir())


def ingest_all(
    data_dir: Path = paths.DEFAULT_DATA_DIR, through_load: int | None = None
) -> list[LoadMetrics]:
    """Ingest every generated load in order, starting from an empty state."""
    loads = available_loads(data_dir)
    if not loads:
        raise FileNotFoundError(f"No loads under {data_dir / 'loads'}; run `generate` first")
    if through_load is not None:
        loads = [load for load in loads if load <= through_load]
    if loads != list(range(len(loads))):
        raise ValueError(f"Loads must be consecutive from 0, found {loads}")

    state: dict[Key, RecordState] = {}
    results = []
    for load_id in loads:
        state, metrics = ingest_load(load_id, data_dir, state)
        results.append(metrics)
    return results
