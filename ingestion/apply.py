"""Steps 2-4 of ingestion: deduplicate, order, and apply events to the state.

The state holds one entry per source key ever seen, including deleted keys
(tombstones). Keeping tombstones is what lets a late, older event be
recognised as stale instead of bringing a deleted record back.

Rules (DESIGN.md section 4):

* **Deduplicate** by ``event_id`` within the batch; the first copy wins.
* **Order** each key's events by ``sequence_no`` (``event_ts`` is ignored).
* **Highest sequence wins**: an event whose ``sequence_no`` is not above the
  key's last applied one is stale and ignored. This single rule handles
  redelivery across loads, out-of-order arrival, updates arriving after a
  delete, and bulk replays.
* **Delete** turns the key into a tombstone.
* **Insert or update** stores the full row image. When the key is new or a
  tombstone, it starts a new *record life* (D-027): a reused source ID is a
  new record, and matching decides who it belongs to.

Because stale events are no-ops, applying the same events twice, or
replaying any earlier load, leaves the state unchanged (idempotency).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from datetime import datetime

VALUE_COLUMNS = ("first_name", "last_name", "dob", "ssn", "gender")

Key = tuple[str, str]


@dataclass(frozen=True)
class RecordState:
    """The current state of one source key."""

    source_system: str
    source_record_id: str
    record_life: int
    is_deleted: bool
    sequence_no: int
    event_id: str
    event_ts: datetime | None
    life_started_load: int | None
    last_changed_load: int
    first_name: str | None = None
    last_name: str | None = None
    dob: str | None = None
    ssn: str | None = None
    gender: str | None = None

    @property
    def key(self) -> Key:
        return (self.source_system, self.source_record_id)


@dataclass
class ApplyStats:
    """What happened to a batch of valid events."""

    duplicates_dropped: int = 0
    stale_ignored: int = 0
    applied: dict[str, int] = field(default_factory=lambda: {"I": 0, "U": 0, "D": 0})
    new_lives: int = 0


def deduplicate(events: Iterable[dict]) -> tuple[list[dict], int]:
    """Keep the first event per ``event_id``; return the survivors and the drop count."""
    seen: set[str] = set()
    unique, dropped = [], 0
    for event in events:
        if event["event_id"] in seen:
            dropped += 1
            continue
        seen.add(event["event_id"])
        unique.append(event)
    return unique, dropped


def apply_event(state: RecordState | None, event: dict, load_id: int) -> RecordState | None:
    """Apply one event to one key's state; return the new state, or None if stale."""
    sequence = int(event["sequence_no"])
    if state is not None and sequence <= state.sequence_no:
        return None

    common = {
        "sequence_no": sequence,
        "event_id": event["event_id"],
        "event_ts": event.get("event_ts"),
        "last_changed_load": load_id,
    }
    if event["op"] == "D":
        if state is None:
            # Delete of a key never seen (its insert may have been rejected):
            # keep a tombstone so older events for it stay ignored.
            return RecordState(
                event["source_system"],
                event["source_record_id"],
                record_life=0,
                is_deleted=True,
                life_started_load=None,
                **common,
            )
        return replace(state, is_deleted=True, **common, **dict.fromkeys(VALUE_COLUMNS))

    values = {c: event.get(c) for c in VALUE_COLUMNS}
    if state is None or state.is_deleted:
        life = (state.record_life if state else 0) + 1
        return RecordState(
            event["source_system"],
            event["source_record_id"],
            record_life=life,
            is_deleted=False,
            life_started_load=load_id,
            **common,
            **values,
        )
    return replace(state, **common, **values)


def apply_events(state: dict[Key, RecordState], events: list[dict], load_id: int) -> ApplyStats:
    """Apply a batch of *validated* events to ``state`` in place."""
    stats = ApplyStats()
    unique, stats.duplicates_dropped = deduplicate(events)

    by_key: dict[Key, list[dict]] = {}
    for event in unique:
        by_key.setdefault((event["source_system"], event["source_record_id"]), []).append(event)

    for key, key_events in by_key.items():
        key_events.sort(key=lambda e: (int(e["sequence_no"]), e["event_id"]))
        current = state.get(key)
        for event in key_events:
            updated = apply_event(current, event, load_id)
            if updated is None:
                stats.stale_ignored += 1
                continue
            stats.applied[event["op"]] += 1
            if not updated.is_deleted and (current is None or current.is_deleted):
                stats.new_lives += 1
            current = updated
        if current is not None:
            state[key] = current
    return stats
