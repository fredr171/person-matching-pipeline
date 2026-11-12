# Running the pipeline

The [README quickstart](../README.md#quickstart) covers the common path with `make`. This page
has everything else: running without `make`, platform notes for Spark, reproducing the
published results, and querying the outputs.

## Without `make` (Windows)

Every `make` target wraps a Python command, so the same workflow runs anywhere:

```powershell
python -m venv .venv
.venv\Scripts\activate
python -m pip install -r requirements-lock.txt
python -m pip install --no-deps -e .
python -m pipeline check-config
python -m pipeline generate --preset small
python -m pipeline ingest
python -m pipeline match-dbt --profile conservative
python -m pipeline match-spark --profile conservative
python -m pipeline compare-engines --profile conservative
python -m pipeline evaluate --profile conservative
python -m pytest
```

`python -m pipeline --help` lists every command, and `python -m pipeline <command> --help`
its options. The matching, comparison and evaluation commands take `--profile`
(conservative, balanced or aggressive), `--data-dir` and `--through-load`.

## Spark notes

- **Requirements.** Java 17 or later. Spark runs in local mode; no cluster or cloud account.
- **Platforms.** Linux, macOS and native Windows. Windows needs no WSL or `winutils`: Spark
  computes every table in memory and the driver writes the files with pyarrow
  ([D-011, D-044](DESIGN.md#18-decision-log)).
- **Speed.** About 25 seconds per small load and 1.5 to 3 minutes per medium load on a
  laptop, against under a minute per medium load in dbt. Most of the difference on
  Windows is starting Python workers, which Windows cannot reuse ([D-046](DESIGN.md#18-decision-log)).
- **Tuning.** Optional environment variables:

  | Variable | Default | Purpose |
  |---|---|---|
  | `PMP_SPARK_MASTER` | `local[*]` | Spark master, e.g. `local[4]` to use four cores |
  | `PMP_SPARK_DRIVER_MEMORY` | `4g` | Driver memory; `1g` works for the small and medium presets |
  | `PMP_SPARK_SHUFFLE_PARTITIONS` | `16` | Shuffle partitions |
  | `PMP_SPARK_ROWS_PER_PYTHON_TASK` | `250000` | Rows per Python task in UDF and clustering steps |

  On a machine short of memory, use `PMP_SPARK_DRIVER_MEMORY=1g` and `PMP_SPARK_MASTER=local[4]`.
- **A killed run.** If a Spark run is killed rather than left to finish, check for a leftover
  `java.exe`: on Windows the Spark JVM can outlive the Python process that started it.

## Reproducing the results

[RESULTS.md](RESULTS.md) is generated from the evaluation outputs (about 40 minutes on a
laptop):

```bash
make generate PRESET=medium
make ingest
make results                       # match and evaluate all three profiles, then render
```

The engine parity table needs a Spark run of each profile and a comparison with dbt (a few
hours on a laptop), then a render without rematching:

```bash
make match-spark PROFILE=balanced  # and conservative, aggressive
make compare-engines PROFILE=balanced
python -m pipeline results
```

## Example queries

Each load's matching outputs are Parquet files in `data/matched/<engine>/<profile>/load_NNNN/`
([all tables](DESIGN.md#13-outputs-per-run)). Any DuckDB session can query them, for example
`python -c "import duckdb; print(duckdb.sql('...'))"` from the repository root. The paths
below are the last load of the small preset.

All pairs blocked as twins, with their names:

```sql
select p.record_key_a, p.record_key_b, a.first_name as first_a, b.first_name as first_b,
       a.last_name, a.dob, p.ssn_level
from 'data/matched/dbt/conservative/load_0012/match_pairs.parquet' p
join 'data/matched/dbt/conservative/load_0012/records_standardized.parquet' a
  on a.record_key = p.record_key_a
join 'data/matched/dbt/conservative/load_0012/records_standardized.parquet' b
  on b.record_key = p.record_key_b
where p.reason_code = 'BLOCKED_TWINS';
```

All matches clustering refused, with the blocked pair that stopped each one:

```sql
select record_key_a, record_key_b, tier,
       conflict_record_key_a, conflict_record_key_b, conflict_reason_code
from 'data/matched/dbt/conservative/load_0012/skipped_joins.parquet';
```

All person IDs merged this load, and the ID each now points to:

```sql
select retired_id, successor_id
from 'data/matched/dbt/conservative/load_0012/person_id_crosswalk.parquet'
where reason = 'MERGED';
```

Merges and deletions across every load:

```sql
select load_id, reason, count(*) as ids
from 'data/matched/dbt/conservative/load_*/person_id_crosswalk.parquet'
group by all
order by all;
```

Why pairs did or did not match, by reason code:

```sql
select metric as reason_code, value as pairs
from 'data/matched/dbt/conservative/load_0012/run_metrics.parquet'
where metric_group = 'reason_code'
order by value desc;
```
