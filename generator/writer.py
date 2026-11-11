"""Write loads to Parquet.

Events go to ``data/loads/load_NNNN/events.parquet`` with a small
``manifest.json`` (load date and event count) that the pipeline may read.
Ground truth goes to the separate ``data/truth/load_NNNN/truth.parquet`` tree.

Schemas are explicit so the files are identical across runs and platforms;
nothing depends on pandas type inference.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from generator.simulate import LoadOutput
from pipeline import paths

EVENT_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("source_system", pa.string()),
        ("source_record_id", pa.string()),
        ("sequence_no", pa.int64()),
        ("event_ts", pa.timestamp("us")),
        ("op", pa.string()),
        ("first_name", pa.string()),
        ("last_name", pa.string()),
        ("dob", pa.string()),
        ("ssn", pa.string()),
        ("gender", pa.string()),
    ]
)

TRUTH_SCHEMA = pa.schema(
    [
        ("source_system", pa.string()),
        ("source_record_id", pa.string()),
        ("true_person_id", pa.string()),
        ("edge_case_tags", pa.list_(pa.string())),
        ("load_id", pa.int64()),
    ]
)


def _table(rows: list[dict], schema: pa.Schema) -> pa.Table:
    columns = {name: [row[name] for row in rows] for name in schema.names}
    return pa.table(columns, schema=schema)


def _write_parquet(table: pa.Table, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="snappy")


class LoadWriter:
    """Writes each load's events, manifest and ground truth."""

    def __init__(self, data_dir: Path = paths.DEFAULT_DATA_DIR) -> None:
        self.data_dir = data_dir

    def reset(self) -> None:
        """Remove previously generated loads and truth (both are reproducible)."""
        for sub in ("loads", "truth"):
            target = self.data_dir / sub
            if target.exists():
                shutil.rmtree(target)

    def write(self, output: LoadOutput) -> None:
        load_dir = paths.load_dir(output.load_id, self.data_dir)
        _write_parquet(_table(output.events, EVENT_SCHEMA), load_dir / "events.parquet")
        manifest = {
            "load_id": output.load_id,
            "load_date": output.load_date.isoformat(),
            "event_count": len(output.events),
        }
        _write_json(load_dir / "manifest.json", manifest)
        truth_path = paths.truth_dir(output.load_id, self.data_dir) / "truth.parquet"
        _write_parquet(_table(output.truth, TRUTH_SCHEMA), truth_path)

    def write_run_manifest(self, manifest: dict) -> None:
        _write_json(self.data_dir / "loads" / "generation.json", manifest)


def _write_json(path: Path, data: dict) -> None:
    # LF line endings so files are byte-identical on Windows and Linux.
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8", newline="\n")
