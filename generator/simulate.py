"""Month-by-month simulation that emits CDC events and ground truth.

Each load runs these steps in a fixed order:

1. Life events that take effect this load are applied; affected records are
   scheduled for updates over the next few loads.
2. **Inserts** for the records scheduled into this load.
3. **Forced updates**: corrections and life-event propagation due this load.
4. **Random updates** fill the rest of the update quota.
5. **Deletes**.
6. Load-level pipeline cases (migration, source ID change).
7. Event-level pipeline cases (encoding corruption, duplicates, column shift,
   bulk replay).

The source-system and pipeline edge cases live here because they act on
events and loads, not on people or records.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

import numpy as np

from generator.context import Context
from generator.model import Override, Person, Record
from generator.record_cases import apply_record_cases
from generator.render import EVENT_VALUE_COLUMNS, SourceState, render
from generator.rng import chance, pick, stream
from pipeline.config import Preset

# Codes implemented in this module (the other modules export their own lists).
PIPELINE_CASES: tuple[str, ...] = (
    "MIGRATION_REFORMAT",
    "SOURCE_ID_CHANGE",
    "SOURCE_ID_REUSE",
    "DUPLICATE_DELIVERY",
    "OUT_OF_ORDER",
    "LATE_AFTER_DELETE",
    "COLUMN_SHIFT",
    "ENCODING_CORRUPTION",
    "BULK_REPLAY",
)

SOURCE_ID_PREFIXES = {"EHR_A": "EA", "CLAIMS_B": "CB", "LAB_C": "LC"}
EVENT_SPACING = timedelta(milliseconds=500)
NEWBORN_FIRST_NAMES = {"M": "Baby Boy", "F": "Baby Girl"}


@dataclass
class LoadOutput:
    """Everything the writer needs for one load."""

    load_id: int
    load_date: date
    events: list[dict]
    truth: list[dict]


@dataclass
class OpCounts:
    inserts: int
    updates: int
    deletes: int


def plan_refresh_ops(ctx: Context, preset: Preset, rng: np.random.Generator) -> list[OpCounts]:
    """Draw insert/update/delete counts for every refresh load up front.

    The record count of a refresh is drawn uniformly from the preset's range
    and split by the configured refresh mix (DESIGN.md section 3.4).
    """
    mix = ctx.config.refresh_mix
    plans = []
    for _ in range(preset.refresh_loads):
        total = int(rng.integers(preset.refresh_records_min, preset.refresh_records_max + 1))
        inserts = round(total * mix.insert)
        updates = round(total * mix.update)
        plans.append(OpCounts(inserts, updates, total - inserts - updates))
    return plans


@dataclass
class Simulation:
    ctx: Context
    refresh_ops: list[OpCounts]
    seed: int
    sources: dict[str, SourceState] = field(init=False)

    def __post_init__(self) -> None:
        cfg = self.ctx.config
        self.sources = {s.name: SourceState.initial(s) for s in cfg.source_systems}
        self.source_names = [s.name for s in cfg.source_systems]
        self.source_weights = np.array([s.weight for s in cfg.source_systems])
        self.rng_records = stream(self.seed, "records")
        self.rng_ops = stream(self.seed, "operations")
        self.rng_pipeline = stream(self.seed, "pipeline")

        self.live: list[Record] = []
        self.live_position: dict[tuple[str, str], int] = {}
        self.current: dict[tuple[str, str], Record] = {}
        self.sequence: dict[tuple[str, str], int] = {}
        self.next_id = dict.fromkeys(self.source_names, 0)
        self.reuse_queue: dict[str, list[tuple[str, int]]] = {s: [] for s in self.source_names}
        self.forced: dict[int, dict[int, Record]] = {}
        self.late: list[dict] = []
        self.delivered: dict[int, list[dict]] = {}
        self.ctx.schedule_update = self.schedule_update

    # ------------------------------------------------------------ bookkeeping

    @property
    def last_load(self) -> int:
        return self.ctx.last_load

    def schedule_update(self, record: Record, load_id: int) -> None:
        self.forced.setdefault(load_id, {})[id(record)] = record

    def _add_live(self, record: Record) -> None:
        self.live_position[record.key] = len(self.live)
        self.live.append(record)
        self.current[record.key] = record

    def _remove_live(self, record: Record) -> None:
        position = self.live_position.pop(record.key)
        last = self.live.pop()
        if last is not record:
            self.live[position] = last
            self.live_position[last.key] = position
        record.alive = False

    def _new_source_id(self, source: str) -> str:
        self.next_id[source] += 1
        return f"{SOURCE_ID_PREFIXES.get(source, source[:2])}{self.next_id[source]:08d}"

    def _next_sequence(self, key: tuple[str, str]) -> int:
        self.sequence[key] = self.sequence.get(key, 0) + 1
        return self.sequence[key]

    # --------------------------------------------------------------- emitting

    def _emit(self, record: Record, op: str, row: dict | None = None) -> dict:
        self.counter += 1
        values = row if row is not None else dict.fromkeys(EVENT_VALUE_COLUMNS)
        event = {
            "event_id": f"E{self.load_id:04d}-{self.counter:09d}",
            "source_system": record.source_system,
            "source_record_id": record.source_record_id,
            "sequence_no": self._next_sequence(record.key),
            "event_ts": self.load_start + EVENT_SPACING * self.counter,
            "op": op,
            **values,
        }
        self.events.append(event)
        self.dirty[id(record)] = record
        return event

    def _render(self, record: Record) -> dict:
        source = self.sources[record.source_system]
        return render(record, self.load_id, source, self.ctx.config.pipeline_cases.migration)

    def _emit_upsert(self, record: Record, op: str) -> dict:
        row = self._render(record)
        record.last_row = row
        self.touched.add(record.key)
        return self._emit(record, op, row)

    def _deliver_late(self, event: dict) -> None:
        """Move an already-emitted event to the next load (or after its successor)."""
        self.events.remove(event)
        if self.load_id < self.last_load:
            self.late.append(event)
        else:
            self.events.append(event)

    # ------------------------------------------------------------------ steps

    def _create_record(self, person: Person) -> Record:
        rng = self.rng_records
        source = self.source_names[int(rng.choice(len(self.source_names), p=self.source_weights))]
        record_id, reused = None, False
        queue = self.reuse_queue[source]
        if queue and queue[0][1] < self.load_id:
            record_id, reused = queue.pop(0)[0], True
        record = Record(source, record_id or self._new_source_id(source), person, self.load_id)

        source_cfg = self.ctx.config.source(source)
        missing_rate = (
            person.ssn_missing_rate
            if person.ssn_missing_rate is not None
            else source_cfg.ssn_missing_rate
        )
        record.flags["ssn_missing"] = chance(rng, missing_rate)
        record.flags["gender_missing"] = chance(rng, source_cfg.gender_missing_rate)
        record.flags["case"] = pick(rng, ("title", "upper", "lower"))
        if person.is_mononym:
            record.flags["mononym_style"] = pick(rng, ("repeat", "lnu", "fnu"))
        if person.newborn_placeholder and self.load_id < self.last_load:
            identity = person.identity_at(self.load_id)
            until = int(rng.integers(self.load_id + 1, min(self.load_id + 3, self.last_load) + 1))
            record.overrides["first"] = Override(
                value=NEWBORN_FIRST_NAMES.get(identity.gender, "Baby"),
                base=identity.first,
                code="NEWBORN_PLACEHOLDER",
                until_load=until,
            )
            self.schedule_update(record, until)
        if reused:
            record.tag("SOURCE_ID_REUSE", self.load_id)
        apply_record_cases(self.ctx, record, rng)
        person.records.append(record)
        return record

    def _apply_life_events(self) -> None:
        rng = self.rng_ops
        for person in self.changes_by_load.get(self.load_id, []):
            for change in person.changes:
                if change.load_id != self.load_id:
                    continue
                person.tag(change.code, self.load_id)
                for record in person.records:
                    if not record.alive:
                        continue
                    self.dirty[id(record)] = record
                    if change.new_records_only:
                        continue
                    if change.code == "DOB_CORRECTED":
                        due = self.load_id
                    else:
                        due = int(
                            rng.integers(self.load_id, min(self.load_id + 2, self.last_load) + 1)
                        )
                    self.schedule_update(record, due)

    def _inserts(self, persons: list[Person]) -> None:
        for person in persons:
            record = self._create_record(person)
            self._add_live(record)
            self._emit_upsert(record, "I")

    def _forced_updates(self) -> int:
        emitted = 0
        rate = self.ctx.rate("OUT_OF_ORDER")
        for record in self.forced.pop(self.load_id, {}).values():
            if not record.alive or record.key in self.touched:
                continue
            previous = record.last_row
            row = self._render(record)
            stale = None
            if previous is not None and row != previous and chance(self.rng_pipeline, rate):
                # The previous image gets an older sequence number but arrives late.
                stale = self._emit(record, "U", previous)
                record.tag("OUT_OF_ORDER", self.load_id)
            self._emit_upsert(record, "U")
            if stale is not None:
                self._deliver_late(stale)
            emitted += 1
        return emitted

    def _pick_untouched(self, count: int) -> list[Record]:
        chosen: list[Record] = []
        attempts = 0
        while len(chosen) < count and attempts < count * 20 and self.live:
            attempts += 1
            record = self.live[int(self.rng_ops.integers(len(self.live)))]
            if record.key not in self.touched:
                self.touched.add(record.key)
                chosen.append(record)
        return chosen

    def _random_updates(self, count: int) -> None:
        for record in self._pick_untouched(count):
            self._emit_upsert(record, "U")

    def _deletes(self, count: int) -> None:
        late_rate = self.ctx.rate("LATE_AFTER_DELETE")
        reuse_rate = self.ctx.rate("SOURCE_ID_REUSE")
        for record in self._pick_untouched(count):
            stale = None
            if record.last_row is not None and chance(self.rng_pipeline, late_rate):
                stale = self._emit(record, "U", record.last_row)
                record.tag("LATE_AFTER_DELETE", self.load_id)
            self._emit(record, "D")
            if stale is not None:
                self._deliver_late(stale)
            self._remove_live(record)
            if chance(self.rng_pipeline, reuse_rate):
                self.reuse_queue[record.source_system].append(
                    (record.source_record_id, self.load_id)
                )

    def _migration(self) -> None:
        rng = self.rng_pipeline
        candidates = [s for s in self.source_names if not self.sources[s].migrated]
        if not candidates or not chance(rng, self.ctx.rate("MIGRATION_REFORMAT")):
            return
        source = pick(rng, candidates)
        self.sources[source].migrate(self.ctx.config.pipeline_cases.migration)
        for record in [r for r in self.live if r.source_system == source]:
            record.tag("MIGRATION_REFORMAT", self.load_id)
            self._emit_upsert(record, "U")

    def _source_id_change(self) -> None:
        rng = self.rng_pipeline
        if not chance(rng, self.ctx.rate("SOURCE_ID_CHANGE")):
            return
        source = pick(rng, self.source_names)
        records = [r for r in self.live if r.source_system == source]
        if not records:
            return
        share = self.ctx.config.pipeline_cases.source_id_change_share
        count = max(1, round(share * len(records)))
        for index in sorted(rng.choice(len(records), size=count, replace=False)):
            old = records[int(index)]
            old.tag("SOURCE_ID_CHANGE", self.load_id)
            self._emit(old, "D")
            self._remove_live(old)
            new = Record(
                source,
                self._new_source_id(source),
                old.person,
                old.created_load,
                overrides=dict(old.overrides),
                flags=dict(old.flags),
                tags=dict(old.tags),
            )
            new.tag("SOURCE_ID_CHANGE", self.load_id)
            old.person.records.append(new)
            self._add_live(new)
            self._emit_upsert(new, "I")

    def _encoding_corruption(self) -> None:
        rate = self.ctx.rate("ENCODING_CORRUPTION")
        for event in self.events:
            if event["op"] == "D":
                continue
            names = (event["first_name"] or "") + (event["last_name"] or "")
            if names.isascii() or not chance(self.rng_pipeline, rate):
                continue
            for column in ("first_name", "last_name"):
                value = event[column]
                if value and not value.isascii():
                    event[column] = value.encode("utf-8").decode("latin-1")
            self._tag_event(event, "ENCODING_CORRUPTION")

    def _duplicates(self) -> None:
        rate = self.ctx.rate("DUPLICATE_DELIVERY")
        for event in list(self.events):
            if not chance(self.rng_pipeline, rate):
                continue
            copy = dict(event)
            self._tag_event(event, "DUPLICATE_DELIVERY")
            if self.load_id < self.last_load and self.rng_pipeline.random() < 0.5:
                self.late.append(copy)
            else:
                self.events.append(copy)

    def _column_shift(self, events: list[dict]) -> None:
        rng = self.rng_pipeline
        if not chance(rng, self.ctx.rate("COLUMN_SHIFT")):
            return
        positions = [i for i, e in enumerate(events) if e["op"] in {"I", "U"}]
        if not positions:
            return
        size = max(
            1, round(self.ctx.config.pipeline_cases.column_shift_batch_share * len(positions))
        )
        start = int(rng.integers(0, max(1, len(positions) - size + 1)))
        for i in positions[start : start + size]:
            event = dict(events[i])
            event["first_name"], event["last_name"], event["dob"], event["ssn"], event["gender"] = (
                "",
                event["first_name"],
                event["last_name"],
                event["dob"],
                event["ssn"],
            )
            events[i] = event
            self._tag_event(event, "COLUMN_SHIFT")

    def _bulk_replay(self, events: list[dict]) -> None:
        rng = self.rng_pipeline
        earlier = [load for load in self.delivered if 1 <= load < self.load_id]
        if not earlier or not chance(rng, self.ctx.rate("BULK_REPLAY")):
            return
        replayed = pick(rng, earlier)
        for event in self.delivered[replayed]:
            events.append(dict(event))
            self._tag_event(event, "BULK_REPLAY")

    def _tag_event(self, event: dict, code: str) -> None:
        record = self.current.get((event["source_system"], event["source_record_id"]))
        if record is not None:
            record.tag(code, self.load_id)
            self.dirty[id(record)] = record

    # -------------------------------------------------------------------- run

    def run(self, persons: list[Person], schedule: list[list[Person]]):
        """Yield one LoadOutput per load, in order."""
        self.changes_by_load: dict[int, list[Person]] = {}
        for person in persons:
            for load in sorted({c.load_id for c in person.changes}):
                self.changes_by_load.setdefault(load, []).append(person)

        for load_id, load_date in enumerate(self.ctx.load_dates):
            self.load_id = load_id
            self.load_start = datetime(load_date.year, load_date.month, load_date.day)
            self.counter = 0
            self.events: list[dict] = []
            self.touched: set[tuple[str, str]] = set()
            self.dirty: dict[int, Record] = {}
            arriving_late, self.late = self.late, []

            self._apply_life_events()
            self._inserts(schedule[load_id])
            if load_id > 0:
                ops = self.refresh_ops[load_id - 1]
                forced = self._forced_updates()
                self._random_updates(max(0, ops.updates - forced))
                self._deletes(ops.deletes)
                self._migration()
                self._source_id_change()
            self._encoding_corruption()
            self._duplicates()

            events = arriving_late + self.events
            if load_id > 0:
                self._column_shift(events)
                self._bulk_replay(events)
                self.delivered[load_id] = events

            yield LoadOutput(load_id, load_date, events, self._truth_rows())

    def _truth_rows(self) -> list[dict]:
        rows = []
        for record in self.dirty.values():
            tags = record.truth_tags(self.load_id)
            if tags == record.last_truth_tags:
                continue
            record.last_truth_tags = tags
            rows.append(
                {
                    "source_system": record.source_system,
                    "source_record_id": record.source_record_id,
                    "true_person_id": record.person.person_id,
                    "edge_case_tags": list(tags),
                    "load_id": self.load_id,
                }
            )
        return rows
