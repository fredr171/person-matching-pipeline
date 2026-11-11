# Person Matching: Design Specification

This document is the source of truth for the project. Code, tests, and the README must agree with it. If an implementation choice conflicts with this spec, update the spec first and record why in the decision log (section 18).

## 1. Goals

Build a portfolio-quality person matching (entity resolution) system that shows the decisions behind the logic, not just the code. The project will be published as a **public repository on my personal GitHub profile**. The audience is potential employers and hiring managers in data engineering and analytics engineering, so the repo must be easy to understand at a glance, easy to run, and clearly documented. See section 19 for GitHub-specific requirements.

- A Python generator produces a synthetic person dataset delivered as incremental CDC loads, with known ground truth.
- An ingestion layer turns raw CDC events into a clean current-state table.
- Matching runs in **two implementations that must produce identical results**: dbt on DuckDB, and PySpark in local mode.
- Matching is **fully automated with no human review queue**. Every decision is stored with a reason code so edge cases can be queried.
- Person IDs stay **stable across loads** and only change when the data justifies it.
- Results are measured against ground truth: pairwise accuracy, per-edge-case outcomes, and entity instability.

Non-goals: a UI, real PII, probabilistic/ML matching, and a human review workflow.

## 2. Platforms and repository layout

- Python 3.11+ for the generator, ingestion, evaluation, and the clustering step.
- dbt-duckdb for the SQL implementation (DuckDB provides `jaro_winkler_similarity`, `levenshtein`, and dbt Python models).
- PySpark in local mode for the distributed implementation.
- Parquet for all data files. YAML for configuration. pytest for tests.
- Everything must run locally from a fresh clone with no cloud account.
- Dependencies are pinned exactly in `pyproject.toml`, with the full transitive set frozen in `requirements-lock.txt` (D-012).
- The `Makefile` is a thin wrapper over a Python CLI (`python -m pipeline <command>`), so every step also runs on Windows without `make` (D-010).
- PySpark needs Java 17+. On Windows, Spark is supported through WSL; Linux CI is the reference environment for the Spark run (D-011).
- Reference CSVs are read as text by every engine (dbt via DuckDB `read_csv`, Python via `csv`), never with type inference (D-018).

```
person-matching-pipeline/
├── README.md
├── LICENSE                    # MIT
├── pyproject.toml             # exact dependency pins, tool config
├── requirements-lock.txt      # full frozen dependency set
├── .github/workflows/ci.yml
├── docs/
│   ├── DESIGN.md              # this file
│   ├── DATA_GENERATION.md     # generator and edge case catalog
│   ├── MATCHING_RULES.md      # rules and their defense
│   ├── RESULTS.md             # metrics per profile, known limitations (generated)
│   └── images/                # generated precision/recall chart
├── config/
│   ├── generator.yaml         # presets, edge case rates, seed
│   ├── matching.yaml          # profiles, thresholds, blocker settings
│   └── reference/             # nicknames, placeholders, particles, suffixes, letters, genders (.csv)
├── pipeline/                  # shared plumbing: config + reference loaders, paths, CLI
├── generator/                 # synthetic data + CDC event generator; name lists in generator/data/
├── ingestion/                 # CDC -> current-state table
├── standardization/           # Python reference implementation (sections 5, 7-9) + similarity functions
├── matching_dbt/              # dbt project (DuckDB); seeds read from config/reference/
├── matching_spark/            # PySpark implementation
├── clustering/                # shared constrained clustering + ID reconciliation
├── evaluation/                # metrics against ground truth
├── tests/
└── Makefile                   # make generate / ingest / match-dbt / match-spark / evaluate / test
```

Reference data (nicknames, placeholders, particles) lives in one place and is read by both implementations so they cannot drift apart.

## 3. Data generator

### 3.1 Output files per load

Each load `N` writes to `data/loads/load_NNNN/`:

- `events.parquet`: the CDC feed the pipeline reads.
- `manifest.json`: load id, load date and event count. The pipeline may read it; the load date is what "today" means for future-DOB and newborn checks (D-026).
- `truth.parquet`: ground truth, written to a separate directory tree (`data/truth/`). **No pipeline code may read truth files.** Only `evaluation/` and tests may.

### 3.2 Event schema (`events.parquet`)

| Column | Type | Notes |
|---|---|---|
| event_id | string | Unique per event; duplicates allowed to simulate redelivery |
| source_system | string | e.g. `EHR_A`, `CLAIMS_B`; used to simulate migrations |
| source_record_id | string | The source's key for the record |
| sequence_no | int64 | Monotonic per source_record_id; authoritative ordering |
| event_ts | timestamp | When the source emitted the event; may arrive out of order |
| op | string | `I`, `U`, or `D` |
| first_name | string | Raw, uncleaned |
| last_name | string | Raw, uncleaned; may contain suffixes |
| dob | string | Raw string; mixed formats allowed |
| ssn | string | Raw; may be numeric-looking, formatted, or malformed |
| gender | string | Raw; `M`, `F`, `U`, `X`, blank, or junk |

**Contract:** `I` and `U` events carry the full row image. `D` events carry only keys. Events that violate the contract (missing keys, column shifts, unparseable values in a way that indicates corruption) are generated deliberately at a low rate to exercise the ingestion layer's rejection logic.

### 3.3 Truth schema (`truth.parquet`)

| Column | Notes |
|---|---|
| source_system, source_record_id | Join key to events |
| true_person_id | The real person this record represents |
| edge_case_tags | List of edge case codes applied to this record (section 3.7) |
| load_id | Load in which this truth row became effective |

A truth row is written when a record life starts and whenever its tag list changes. The truth for a record at load N is its latest row with `load_id <= N`; a reused source ID (SOURCE_ID_REUSE) therefore maps to a different person from the load it is reused (D-025).

### 3.4 Presets

| Preset | Initial records | Records per refresh | Purpose |
|---|---|---|---|
| small | 1,000 | ~50 | Unit tests, README examples |
| medium (default) | 100,000 | 2,000–5,000 | Main demo and metrics |
| large | 5,000,000 | ~100,000 | PySpark scale demonstration |

Refresh mix: about 70% inserts, 25% updates, 5% deletes (configurable).

### 3.5 Population model

- Generate a population of true people first, then emit records for them.
- Records per person: most have 1, a meaningful share have 2–5, a small tail has many (configurable distribution). Average about 1.4 at the medium preset.
- Names drawn from frequency-weighted lists, including Hispanic, East Asian, South Asian, Arabic, and European names so cultural edge cases occur naturally.
- DOBs across a realistic age range, including newborns.
- SSNs: valid-format, unique per true person (except where an edge case says otherwise). Do not generate SSNs in reserved ranges except as placeholders.
- A `--seed` makes the full sequence of loads byte-for-byte reproducible.

### 3.6 Life-event timeline

Each refresh simulates roughly one month. Edge cases are applied as **events on a person's timeline**, and they compound: one person may marry, then have their source system migrated (truncating their name), then have an SSN corrected, with the correction arriving out of order. The generator must support stacking events on the same person.

### 3.7 Edge case catalog

All rates live in `config/generator.yaml` and are **deliberately exaggerated** for testing. `docs/DATA_GENERATION.md` must render this catalog with the configured rates, generated from the config so docs cannot drift from code.

Each entry has: a code, description, how it is generated, and the expected outcome (rule and reason code that should handle it).

**Identifier errors and placeholders**

| Code | Description | Expected outcome |
|---|---|---|
| SSN_TYPO_SUB | One digit substituted | Tier 2 match |
| SSN_TYPO_TRANSPOSE | Adjacent digits transposed | Tier 2 match |
| SSN_NUMERIC_CORRUPT | Leading zeros lost, `.0` suffix, scientific notation | Fixed by standardization, then normal tiers |
| SSN_PLACEHOLDER | 000-00-0000, 123-45-6789, 999-99-9999, repeated digits | Treated as missing |
| SSN_TEMP_ITIN | Temporary ID or ITIN (starts with 9) | Treated as missing |
| SSN_DELAYED | Record first has no SSN, later update adds it | Same source record updates; matching uses new SSN |
| SSN_CORRECTED | Wrong SSN later corrected by source | Rematch reflects corrected value |
| DOB_TYPO_DIGIT | One digit wrong | Tier 3 match |
| DOB_MONTH_DAY_SWAP | 03/04 vs 04/03 | Tier 3 match |
| DOB_YEAR_OFF_ONE | Year ±1, same month/day | Tier 3 match |
| DOB_LEAP_DAY | Feb 29 recorded as Feb 28 or Mar 1 | Tier 3 match |
| DOB_PLACEHOLDER | 1900-01-01 or other default | Treated as missing |
| DOB_JAN_FIRST | Jan 1 used when only year known | Kept, flagged weak |
| DOB_TWO_DIGIT_YEAR | `85` parsed as 2085 | Future DOB treated as missing |
| DOB_YEAR_ONLY | Only a year provided | Treated as missing |
| DOB_CORRECTED | DOB legally corrected later | Rematch reflects corrected value |

**Name variation**

| Code | Description | Expected outcome |
|---|---|---|
| NAME_TYPO | Misspelling, duplicated or dropped letter | Strong name via fuzzy |
| NAME_NICKNAME | Dave/David, Bob/Robert, Peggy/Margaret | Strong name via nickname table |
| NAME_PHONETIC | Catherine/Katherine, Smith/Smyth | Strong name via phonetic |
| NAME_TRANSLITERATION | Mohammed/Muhammad/Mohamed | Strong name via variant table |
| NAME_SWAPPED | First and last reversed | Swapped level; matches per tiers |
| NAME_FAMILY_FIRST | Systematic family-name-first ordering from one source | Swapped level |
| NAME_MIDDLE_IN_FIRST | "Mary Ann" in first name | Token match |
| NAME_CONCATENATED | MaryAnn vs Mary Ann | Space-removed comparison |
| NAME_INITIAL | "D." vs David | Weak name |
| NAME_TRUNCATED | Christopher → Christoph | Prefix rule (≥ 6 chars) |
| NAME_COMPOUND_SURNAME | García López vs López | Token match |
| NAME_PARTICLE | De la Cruz / Delacruz / Cruz | Particle-stripped comparison |
| NAME_UNICODE | José vs Jose; composed vs decomposed | NFC normalize, strip accents |
| NAME_SUFFIX_IN_LAST | "Smith Jr." / "Smith, Jr" in last name | Suffix extracted in standardization |
| NAME_PLACEHOLDER | Baby Boy, Infant, Unknown, FNU, LNU, Test User, Asdf | Treated as missing |
| NAME_MONONYM | Single legal name repeated or LNU-filled | Placeholder handling; documented limitation |

**Life and legal events**

| Code | Description | Expected outcome |
|---|---|---|
| MARRIAGE | Last name changes | Tier 1 (one strong name) |
| DIVORCE_RESTORE | Last name changes back | Tier 1 |
| LEGAL_FIRST_NAME_CHANGE | First name changes, often with gender | Tier 1 (one strong name) |
| ADOPTION_NAME_CHANGE | Child's last name changes | Tier 1 |
| GENDER_CHANGE_OR_ERROR | Gender value differs | Ignored as evidence |
| FULL_NAME_CHANGE | Both names change, SSN and DOB same | Known limitation: no match |
| SSN_REPLACEMENT | SSA issues a new SSN | Known limitation unless names and DOB carry it |

**Family and look-alike cases**

| Code | Description | Expected outcome |
|---|---|---|
| TWINS | Same DOB and last name, different first names, near SSNs | `BLOCKED_TWINS` |
| MULTIPLES | Triplets or more | `BLOCKED_TWINS` |
| NEWBORN_PARENT_SSN | Newborn recorded with parent's SSN | `BLOCKED_GENERATIONAL` |
| NEWBORN_PLACEHOLDER | Baby Boy Smith later updated with real name | Placeholder name handling |
| JR_SR | Father and son, same name, maybe borrowed SSN | `BLOCKED_SUFFIX` or `BLOCKED_GENERATIONAL` |
| COMMON_NAME_SAME_DOB | Unrelated people, same common name and DOB | No SSN: Tier 4 no-match (conservative) |
| IDENTICAL_STRANGERS | Two people with identical five fields | Known limitation |

**Fraud**

| Code | Description | Expected outcome |
|---|---|---|
| SSN_SHARED_FRAUD | Different person uses victim's SSN with own name/DOB | `BLOCKED_SSN_CONFLICT` |
| SSN_SHARED_MANY | Several fraudsters share one SSN | `BLOCKED_SSN_CONFLICT`; oversized SSN block monitored |
| SYNTHETIC_IDENTITY | Real SSN combined with another person's name and DOB | `BLOCKED_SSN_CONFLICT` where detectable |
| FULL_IDENTITY_COPY | All five fields copied | Known limitation |
| NEAR_MATCH_ATTACK | One character changed from a real identity | Measured; conservative thresholds limit exposure |

**Source system and pipeline**

| Code | Description | Expected outcome |
|---|---|---|
| MIGRATION_REFORMAT | Source migration changes formats, truncates names, introduces defaults | Standardization + normal tiers |
| SOURCE_ID_CHANGE | Migration assigns new source_record_ids | Matching on attributes, not source IDs |
| SOURCE_ID_REUSE | Deleted source ID reused for a different person | Ingestion treats post-delete insert as a new record version; matching decides |
| DUPLICATE_DELIVERY | Same event delivered twice | Deduplicated by event_id |
| OUT_OF_ORDER | Older event arrives after newer one | Ordered by sequence_no |
| LATE_AFTER_DELETE | Update with older sequence_no arrives after delete | Ignored; record stays deleted |
| COLUMN_SHIFT | Batch rows shifted by one column | Rejected with reason |
| ENCODING_CORRUPTION | Mojibake (e.g. Latin-1 read as UTF-8) | Rejected or flagged, per validation rules |
| BULK_REPLAY | Source replays historical events | Idempotent; no change |

## 4. Ingestion layer (CDC → current state)

Shared Python module, run before either matching implementation. For each load it writes to `data/ingested/load_NNNN/`:

- `current_records.parquet`: one row per live source record. Input to matching.
- `rejects.parquet`: events rejected in this load, with reason code and detail.
- `state.parquet`: every key ever seen, including tombstones. Input to the next load.
- `metrics.json`: events read, rejected (per reason), duplicates dropped, stale events ignored, events applied, live records and tombstones.

1. **Validate** each event against the schema contract. Reject with a reason code (`REJECT_MISSING_KEY`, `REJECT_COLUMN_SHIFT`, `REJECT_ENCODING`, `REJECT_BAD_OP`). Rejections are logged, not queued for review.
2. **Deduplicate** by `event_id`.
3. **Order** by `sequence_no` per `(source_system, source_record_id)`. `event_ts` is informational only.
4. **Apply** events to the prior current state. The highest sequence_no wins. A delete creates a tombstone; any event with a lower sequence_no than the tombstone is ignored. An insert with a higher sequence_no after a delete starts a new record life. An update for an unknown or deleted key is applied as an upsert (D-027).
5. **Idempotency:** reprocessing any load, or replaying old loads, must produce the same current state.

## 5. Standardization

Applied identically in both implementations. Raw values are kept alongside cleaned values. The Python reference implementation in `standardization/` defines the expected behavior of sections 5 and 7–9, and both engines are tested against it (D-037).

- Unicode NFC normalization, then strip accents through `config/reference/letters.csv` (D-029); uppercase; trim and collapse whitespace; remove punctuation except hyphens and apostrophes inside names (then compare with and without them).
- Extract suffixes (JR, SR, II, III, IV, in all common formats) from last and first names into `suffix`.
- Strip name particles (DE, DE LA, DEL, VAN, VAN DER, VON, AL, EL, DA, DI, LE, LA, O') into a particle-free comparison form; keep the original too.
- Tokenize names; keep a space-removed form.
- SSN: keep digits only; if numeric artifacts are detected, recover (drop `.0`, expand scientific notation where lossless), left-pad to 9. Null out placeholders, invalid areas (000, 666), group 00, serial 0000, and anything starting with 9.
- DOB: parse common formats; null out placeholders, future dates, year-only values; flag Jan 1 as weak.
- Placeholder names (from `config/reference/placeholders.csv`) become null.
- Gender normalized to M, F, X, or null through `config/reference/genders.csv`.

Details the bullets above leave open (comparison forms, which SSNs are padded, accepted DOB formats, null reason codes) are fixed in D-030 and D-038.

## 6. Blocking

Candidate pairs are generated from records sharing any of these keys (skipping null keys):

| Key | Catches |
|---|---|
| ssn | Tier 1 and 3, SSN conflicts, generational cases |
| dob | Tier 2 and 4, twins |
| sorted digits of ssn + birth year | Transpositions with DOB errors |
| ssn first 5 + dob year | Substitutions in last 4 |
| phonetic(last) + first initial + birth year | Name-led matches with DOB errors |

Blocks larger than a configured cap are skipped and reported in metrics (a spike usually means a new placeholder value). Pairs are deduplicated across keys. Exact key values are fixed in D-040.

## 7. Field comparison levels

**SSN:** `EXACT`, `NEAR` (one substitution or one adjacent transposition), `MISSING`, `DIFFERENT`.

**DOB:** `EXACT`, `NEAR` (month/day swap, one-digit typo, year ±1 with same month/day, leap-day shift), `MISSING`, `DIFFERENT`. Also computed: absolute gap in years.

**Names** (first and last separately, best result across all forms and tokens; forms in D-030):

| Level | Rule |
|---|---|
| STRONG | Exact, nickname/variant table (names in the same group, D-031), phonetic (Double Metaphone), Jaro-Winkler ≥ strong threshold, or prefix match where the shorter name is ≥ 6 chars |
| WEAK | Jaro-Winkler between weak and strong thresholds, or initial only (a single letter is never STRONG) |
| NONE | Anything else, or missing |

A first/last swap is detected separately: if first↔last cross-comparisons are both STRONG, both names are treated as STRONG with flag `SWAPPED`.

Gender-ambiguous nicknames (Chris, Alex, Sam, Pat, Jo, and others in the reference table) matched via nickname with conflicting non-null genders are downgraded to WEAK.

**Gender** is never positive evidence.

## 8. Match tiers

Principle: **one imperfect field per auto-match.** Uncertain cases are no-match.

| Tier | SSN | DOB | Names | Outcome | Reason code |
|---|---|---|---|---|---|
| 1 | EXACT | EXACT | At least one STRONG | Match | `TIER_1_EXACT_IDS` |
| 2 | NEAR | EXACT | Both STRONG | Match | `TIER_2_SSN_NEAR` |
| 3 | EXACT | NEAR | Both STRONG | Match | `TIER_3_DOB_NEAR` |
| 4 | MISSING | EXACT | Both STRONG | Per profile (no-match in conservative) | `TIER_4_NO_SSN` |
| — | Anything else | | | No match | `NO_MATCH_INSUFFICIENT` |

A January 1 DOB (DOB_JAN_FIRST) is weak: it counts as EXACT in Tier 1, where the SSN is also exact, but not in Tiers 2 and 4 (D-032). Tier 4 keeps its reason code when the profile does not allow it to match (D-033).

Tier 1 deliberately requires only one strong name: exact SSN and exact DOB together are strong enough to survive marriage, divorce, adoption, and legal first-name changes. Twins (different SSNs) and parent/child cases (different DOBs) cannot reach Tier 1.

## 9. Blockers (cannot-link rules)

Evaluated on every candidate pair. A blocker overrides any tier and creates a **cannot-link** constraint used in clustering.

| Blocker | Condition | Reason code |
|---|---|---|
| Generational | SSN EXACT or NEAR, DOB gap ≥ 12 years | `BLOCKED_GENERATIONAL` |
| Suffix conflict | Both suffixes non-null and different | `BLOCKED_SUFFIX` |
| Twins/multiples | DOB EXACT, last STRONG, first NONE with both first names present (D-035), SSN NEAR or both non-null and differing, not EXACT (D-036) | `BLOCKED_TWINS` |
| SSN conflict | SSN EXACT, DOB DIFFERENT, both names NONE | `BLOCKED_SSN_CONFLICT` |
| Newborn on SSN | One record has a placeholder name and DOB within the last year, other record's DOB ≥ 12 years older | `BLOCKED_GENERATIONAL` |

## 10. Clustering

Pairwise matches are not transitive by default. Clustering is constrained and deterministic:

1. Compute connected components over all match edges (this bounds the work).
2. Within each component, sort match edges by tier ascending, then by `(min record key, max record key)` for a stable order.
3. Greedy union-find: join two clusters only if no cannot-link pair exists between any member of one and any member of the other. Skipped joins are recorded with `SKIPPED_TRANSITIVITY`.

A record key is `SOURCE|ID`, and a cluster id is the smallest record key in the cluster (D-041). Each skipped join records the cannot-link pair that stopped it.

Implementation: standardization, blocking, comparison, tiers, and blockers are SQL in dbt and DataFrame operations in PySpark. The constrained clustering step is sequential by nature, so both implementations call the **same shared Python function** in `clustering/`: a dbt Python model on DuckDB, and `groupBy(component).applyInPandas` in PySpark. This is a deliberate, documented decision: identical logic beats forcing a greedy algorithm into SQL.

The same reasoning applies to the two string-similarity primitives that the engines do not both provide natively: Jaro-Winkler (DuckDB has it, Spark does not) and Double Metaphone (neither has it). Both are implemented once in `standardization/similarity.py` and registered as a DuckDB UDF for dbt and as a pandas UDF in PySpark. Everything else stays in SQL / DataFrame operations (D-009).

## 11. Stable person ID reconciliation

Matching recomputes everything each load. IDs are then reconciled against the previous load so only records whose cluster changed get new IDs.

| Situation | Rule |
|---|---|
| New cluster's records came from one previous ID | Keeps that ID |
| New cluster contains records from several previous IDs | Oldest ID survives; others retired in crosswalk pointing to survivor |
| A previous ID's records are now in several clusters | Cluster holding the most of them keeps the ID; others get new IDs |
| Cluster has no previously-assigned records | New ID |
| All of a previous ID's records deleted | ID retired with no successor |

Ties go to the oldest ID. Retired IDs are never revived. IDs are assigned from a monotonic sequence with a fixed prefix (e.g. `P000000123`).

Records are identified by `SOURCE|ID|LIFE`, so a reused source ID is a new record. When a split leaves equal parts, the part in the cluster with the smallest cluster id keeps the ID. New IDs are issued in cluster id order (D-041).

Outputs:
- `person_assignment` (source record → person_id, load_id)
- `person_id_crosswalk` (retired_id, successor_id or null, load_id, reason: `MERGED` or `ALL_DELETED`)

## 12. Profiles

| Profile | Strong name threshold | Weak name range | Tier 4 | Use case |
|---|---|---|---|---|
| **conservative (default)** | ≥ 0.95 | 0.88–0.95 | No match | Healthcare: a false merge can put the wrong allergies or medications on a chart |
| balanced | ≥ 0.92 | 0.85–0.92 | No match | General purpose |
| aggressive | ≥ 0.88 | 0.82–0.88 | Match | Marketing: duplicates are cheap |

Generational gap (12 years) and block size cap are also in `config/matching.yaml`.

## 13. Outputs per run

- `match_pairs`: every compared pair with field levels, tier, blocker, final decision, reason code.
- `clusters`, `person_assignment`, `person_id_crosswalk`.
- `rejects` from ingestion.
- `run_metrics`: counts by reason code, skipped blocks, cluster size distribution.

Each engine writes one Parquet file per table to `data/matched/<engine>/<profile>/load_NNNN/`. Besides the tables above this includes `records_standardized`, `skipped_joins`, `skipped_blocks`, and `id_state`, which holds the next ID sequence number for the following load (D-042).

Users must be able to pull edge cases with simple queries, e.g. all `BLOCKED_TWINS` pairs, all `SKIPPED_TRANSITIVITY` joins, all ID merges this load. Provide example queries in the README.

## 14. Evaluation

Run per load and per profile, reading truth only in `evaluation/`:

- **Pairwise** precision, recall, F1 over record pairs (true same-person pairs vs predicted).
- **Per edge case**: for each edge case code, how many affected records ended up correctly matched or correctly separated.
- **Entity instability**: for each record present in consecutive loads, did its person_id change? Classify each change as *corrective* (the record's pairwise F1 against truth improved), *degrading* (worsened), or *neutral*. Report churn rate and the corrective/degrading split.
- A summary table and a precision/recall chart across the three profiles for `docs/RESULTS.md`.

`python -m pipeline evaluate` writes each load's results to `data/evaluation/<engine>/<profile>/load_NNNN/`, and `python -m pipeline results` renders `docs/RESULTS.md` and its chart from them. Exact definitions are in D-043.

## 15. Testing

- **Unit tests** for every standardization function and comparison level.
- **Scenario tests**: one small hand-built dataset per edge case code asserting the expected reason code.
- **Ingestion tests**: duplicate delivery, out-of-order, late-after-delete, replay, column shift.
- **Idempotency**: running the same load twice changes zero person IDs.
- **Locality**: a change to one person's records only changes IDs within the affected clusters.
- **Parity**: dbt and PySpark outputs (`match_pairs` decisions, `person_assignment`) are identical on the small and medium presets.
- **Determinism**: same seed → identical generated data and identical results.

## 16. Known limitations (document in README)

Not solvable with five fields: full identity copies, unrelated people with identical values, SSN replacement combined with name changes, simultaneous change of first and last name, and mononyms handled with placeholder fill. Each is generated and measured so the README can state its actual impact.

## 17. Build phases

Each phase ends with passing tests and updated docs.

1. Repo skeleton, configs, reference data, Makefile.
2. Generator with ground truth, presets, seed, timeline events, and the full edge case catalog. Generate `DATA_GENERATION.md` from config.
3. Ingestion layer with tests.
4. Shared standardization reference (Python) with unit tests; this defines expected behavior for both implementations.
5. dbt-duckdb implementation through match_pairs; shared clustering; ID reconciliation.
6. Evaluation module and metrics.
7. PySpark implementation and parity tests.
8. Large preset, scale run, `RESULTS.md`, README polish.

## 18. Decision log

Record each significant decision with date, decision, alternatives considered, and reason. Entries are never deleted; a reversed decision gets a new entry that supersedes the old one.

**D-001 · 2026-11-11 · No human review queue**
- *Decision:* Every pair is decided automatically; uncertain pairs become no-match with a reason code.
- *Alternatives:* A review queue for borderline pairs (the common production pattern).
- *Reason:* A queue hides the hard cases behind a human. Storing every decision with a reason code keeps edge cases queryable and makes the matcher's behaviour fully measurable against ground truth.

**D-002 · 2026-11-11 · Uncertain means no-match**
- *Decision:* Any pair not meeting a tier is `NO_MATCH_INSUFFICIENT`.
- *Alternatives:* Scoring models that match above a probability cut-off.
- *Reason:* In the target domain (healthcare) a false merge is far more harmful than a duplicate. Duplicates can be merged later; a merged chart can cause clinical harm.

**D-003 · 2026-11-11 · Tier 1 requires only one strong name**
- *Decision:* Exact SSN + exact DOB + one STRONG name is a match.
- *Alternatives:* Require both names strong for every tier.
- *Reason:* Marriage, divorce, adoption and legal first-name changes alter exactly one name. Exact SSN and DOB together are rare enough to carry the match; twins (different SSNs) and parent/child (different DOBs) cannot reach Tier 1, and blockers catch SSN fraud.

**D-004 · 2026-11-11 · Full rematch each load**
- *Decision:* Matching recomputes all pairs every load; stability comes from ID reconciliation, not from incremental matching.
- *Alternatives:* Incremental matching of only new/changed records against existing clusters.
- *Reason:* Full rematch is simpler, order-independent and makes corrections (e.g. SSN_CORRECTED) automatically reflected. It is affordable at the target scales, and reconciliation keeps IDs stable.

**D-005 · 2026-11-11 · ID reconciliation rules (section 11)**
- *Decision:* Majority/oldest-ID-survives reconciliation with a retire-only crosswalk.
- *Alternatives:* Always mint new IDs; keep the ID of the lowest record key.
- *Reason:* Downstream systems hold person IDs. Only changes justified by the data should change them, and every change must be traceable through the crosswalk. "Oldest survives" is deterministic and favours the ID most likely to be referenced elsewhere.

**D-006 · 2026-11-11 · Shared Python clustering**
- *Decision:* Both implementations call one Python function for constrained clustering.
- *Alternatives:* Recursive SQL / iterative DataFrame implementations per engine.
- *Reason:* Greedy constrained union-find is inherently sequential. Two independent implementations would be hard to keep identical; one shared function guarantees parity by construction.

**D-007 · 2026-11-11 · Conservative default profile**
- *Decision:* The default profile is `conservative` (strong ≥ 0.95, Tier 4 off).
- *Alternatives:* `balanced` as default.
- *Reason:* Consistent with D-002 and the healthcare framing. Other profiles exist to show the precision/recall trade-off.

**D-008 · 2026-11-11 · Exaggerated edge case rates**
- *Decision:* Edge case rates in `config/generator.yaml` are far higher than real-world rates.
- *Alternatives:* Realistic rates.
- *Reason:* At realistic rates many edge cases would appear a handful of times or never, so per-edge-case metrics would be noise. Headline metrics are therefore not a forecast of production accuracy; `RESULTS.md` says so.

**D-009 · 2026-11-11 · Shared Python similarity functions**
- *Decision:* Jaro-Winkler (rapidfuzz) and Double Metaphone (`metaphone` package) are implemented once in `standardization/similarity.py` and registered as a DuckDB UDF (dbt) and a pandas UDF (PySpark).
- *Alternatives:* DuckDB's native `jaro_winkler_similarity` in dbt plus a UDF in Spark; Soundex instead of Double Metaphone.
- *Reason:* Spark has no Jaro-Winkler and neither engine has Double Metaphone. Two different Jaro-Winkler implementations can disagree in the last decimal places, which flips decisions at the 0.95 threshold and breaks parity. One implementation removes that risk, as in D-006.

**D-010 · 2026-11-11 · Makefile wraps a Python CLI**
- *Decision:* Every Makefile target calls `python -m pipeline <command>`.
- *Alternatives:* Logic in the Makefile; require `make` on Windows.
- *Reason:* `make` is not available on Windows by default. The CLI gives one portable entry point, and the Makefile stays a convenience for Linux/macOS and CI.

**D-011 · 2026-11-11 · PySpark on Windows via WSL**
- *Decision:* The Spark implementation is supported on Linux/macOS and on Windows via WSL; Linux CI is the reference run.
- *Alternatives:* Bundle `winutils.exe`/`HADOOP_HOME` setup instructions; Docker.
- *Reason:* Native Windows Spark needs Hadoop binaries to write files, which is brittle and unrelated to the project's purpose. WSL adds no project-specific setup.
- *Update (phase 1):* In-memory Spark jobs do run on native Windows once the driver is bound to `127.0.0.1` (Spark rejects hostnames containing underscores), so the toolchain smoke test runs everywhere. Writing files still needs winutils; this will be revisited in phase 7.

**D-012 · 2026-11-11 · pip with exact pins and a lock file**
- *Decision:* Direct dependencies pinned with `==` in `pyproject.toml`; full transitive set frozen in `requirements-lock.txt`.
- *Alternatives:* uv / Poetry lock files.
- *Reason:* Works with the pip that ships with Python, so a fresh clone needs no extra tool.

**D-013 · 2026-11-11 · Layout additions**
- *Decision:* Added `pipeline/` (config, reference loaders, paths, CLI), `standardization/` (Python reference for section 5, plus D-009 functions) and `config/reference/suffixes.csv`. Moved this file to `docs/` as the layout specifies.
- *Alternatives:* Put shared code in `ingestion/`; hardcode suffix forms.
- *Reason:* Shared plumbing should not live inside one pipeline stage. Suffix forms are reference data like particles and belong where both implementations read them.

**D-014 · 2026-11-11 · No Faker in the generator**
- *Decision:* Names come from frequency-weighted lists committed to the repo, sampled with a seeded NumPy generator.
- *Alternatives:* Faker.
- *Reason:* Faker's output can change between versions, which would break byte-for-byte reproducibility (section 3.5). Curated lists also control the mix of cultures needed for name edge cases.

**D-015 · 2026-11-11 · Gender-ambiguous nicknames are derived, not flagged**
- *Decision:* `nicknames.csv` records the gender of each canonical name. A variant is gender-ambiguous when it maps to canonical names of both genders (e.g. CHRIS → CHRISTOPHER and CHRISTINA).
- *Alternatives:* A hand-maintained `gender_ambiguous` column.
- *Reason:* A derived property cannot drift out of sync with the rows it describes.

**D-016 · 2026-11-11 · Edge case catalog lives in `generator.yaml`**
- *Decision:* Each edge case entry in `generator.yaml` holds its code, category, description, expected outcome, rate and the unit the rate applies to (`person`, `record`, `event` or `load`). `DATA_GENERATION.md` is rendered from it. A test checks that the codes match section 3.7 exactly.
- *Alternatives:* Rates in YAML, descriptions in Markdown.
- *Reason:* Section 3.7 requires the docs to be generated from the config. Keeping everything in one place makes drift impossible, and the test catches drift between the config and this spec.

**D-017 · 2026-11-11 · Unspecified parameters given defaults**
- *Decision:* 12 refresh loads per run (one simulated year); block size cap 1,000 records; newborn window 1 year; person IDs `P` + 9 digits.
- *Alternatives:* —
- *Reason:* The spec did not fix these values. All are configurable, and the README reports the values used.

**D-018 · 2026-11-11 · dbt reads reference data as external text sources, not seeds**
- *Decision:* dbt reads `config/reference/*.csv` with DuckDB `read_csv(..., all_varchar = true)` through a dbt-duckdb external source. Python reads the same files with the `csv` module, not pandas.
- *Alternatives:* dbt seeds with `seed-paths` pointing at `config/reference/`; pandas `read_csv`.
- *Reason:* Verified in phase 1: dbt's seed CSV parser turns the placeholder value `NULL` into a real null, and pandas does the same for `NA` and `NULL` by default. A placeholder list that silently loses entries is exactly the kind of drift the shared reference data exists to prevent. A smoke test asserts that dbt and the Python loader see identical rows.

**D-019 · 2026-11-11 · pandas pinned to 2.3.3, not 3.x**
- *Decision:* Pin `pandas==2.3.3`.
- *Alternatives:* pandas 3.1.1 (latest).
- *Reason:* PySpark 4.3.0 emits "PySpark does not yet fully support pandas >= 3.0.0" and recommends pandas < 3. `applyInPandas` is central to the shared clustering (D-006), so the supported version wins. The pytest config turns that warning into a failure, so an accidental upgrade is caught.

**D-020 · 2026-11-11 · At most one distortion per field per record**
- *Decision:* A record field (first name, last name, SSN, DOB, gender, name order) carries at most one distortion. Distortions of different fields stack, and life events stack on a person.
- *Alternatives:* Let every case apply independently, so one SSN could get a typo and a placeholder.
- *Reason:* Two distortions on one field make the tag meaningless. A "typo'd placeholder" is neither a typo nor a placeholder, and the expected outcome in section 3.7 can't be checked. Stacking across fields still produces compound cases, such as an SSN typo plus a nickname.

**D-021 · 2026-11-11 · Generator draws nicknames and transliterations from the matcher's table**
- *Decision:* `NAME_NICKNAME` and `NAME_TRANSLITERATION` use `config/reference/nicknames.csv`. Phonetic variants use a separate generator list that a test verifies against Double Metaphone.
- *Alternatives:* An independent nickname source for the generator, so some variants are unknown to the matcher.
- *Reason:* Section 3.7 defines the expected outcome as "via nickname table", which requires table coverage. The circularity is stated in `DATA_GENERATION.md`: measured recall on these two cases is an upper bound.

**D-022 · 2026-11-11 · Per-source formatting and missingness as baseline noise**
- *Decision:* Each source has its own name casing, DOB and SSN format, and missing-SSN and missing-gender rates (`source_systems` in `generator.yaml`). These are not edge cases and are not tagged.
- *Alternatives:* One uniform format, with all variation coming from tagged edge cases.
- *Reason:* Real sources always disagree on formats. Missing SSNs are needed to exercise Tier 4 and COMMON_NAME_SAME_DOB. Standardization must handle every format.

**D-023 · 2026-11-11 · How edge cases are applied and tagged**
- *Decision:* Population, timeline, record and pipeline cases are applied by separate modules, and a test checks they cover the catalog exactly. A case is tagged on every record it affects. For relationship cases (twins, fraud, JR/SR, copies) that means *both sides*. Life events are tagged from the load they happen. Rates for cases that only some units qualify for were raised so each one is measurable at the medium preset.
- *Alternatives:* Tag only the "deviant" side (e.g. only the fraudster).
- *Reason:* Evaluation asks whether affected records ended up correctly matched *or correctly separated*. The victim of SSN fraud is as much at risk of a false merge as the fraudster.

**D-024 · 2026-11-11 · Refresh size counts base operations only**
- *Decision:* "Records per refresh" (section 3.4) is the number of base inserts, updates and deletes. Edge-case events come on top: corrections, migration re-emits, re-keys, duplicates, replays and late arrivals. A migration re-emits every live record of the migrated source.
- *Alternatives:* Squeeze edge-case events into the refresh quota.
- *Reason:* A migration or replay is by nature a burst far larger than a normal month. Capping it would make it unrealistic.

**D-025 · 2026-11-11 · Truth rows are versioned by load**
- *Decision:* A new truth row is written when a record life starts or its tag list changes. The latest row with `load_id <= N` is the truth at load N.
- *Alternatives:* A full truth snapshot per load.
- *Reason:* Matches the section 3.3 schema ("load in which this truth row became effective"), handles reused source IDs, and keeps truth files small.

**D-026 · 2026-11-11 · Per-load manifest with the load date**
- *Decision:* Each load directory has a `manifest.json` with its load date (load 0 = `simulation_start`, then monthly). The pipeline may read it.
- *Alternatives:* Use the wall-clock date; infer the date from `event_ts`.
- *Reason:* Standardization needs "today" to null out future DOBs and to detect newborns. The wall clock would make results depend on when they are run, and `event_ts` is informational only (section 4).

**D-027 · 2026-11-11 · Record lives, upserts and persisted tombstones**
- *Decision:* Ingestion keeps one state row per key ever seen, tombstones included, in `state.parquet` between loads. Any I or U for a new or deleted key starts a new record life (`record_life` counts them); a U for an unknown key is applied as an upsert because it carries the full row. A D for an unknown key leaves a tombstone. Equal sequence numbers are ordered by `event_id`.
- *Alternatives:* Drop updates for unknown keys; forget tombstones after the load that created them.
- *Reason:* Section 3.2 guarantees full row images, so an upsert loses nothing, and it recovers records whose insert was rejected. Persisted tombstones are what make a late update after a delete (LATE_AFTER_DELETE) and replays of old loads no-ops.

**D-028 · 2026-11-11 · Structural validation only; mojibake is rejected**
- *Decision:* Validation rejects broken *structure*, never bad *values*: placeholder SSNs, impossible DOBs and odd formats pass to standardization. Column shift needs a DOB containing letters plus a corroborating sign (empty first name, date-like SSN, or identifier-like gender). Mojibake (a UTF-8 lead byte followed by a continuation byte, decoded as Latin-1) and control or replacement characters are rejected as `REJECT_ENCODING`, resolving "rejected or flagged" in section 3.7 to rejected.
- *Alternatives:* Reject any unparseable DOB as corruption; repair mojibake by re-decoding.
- *Reason:* A single bad DOB is a data quality case the catalog expects matching to handle (DOB_PLACEHOLDER, DOB_YEAR_ONLY), so it must not be confused with a shift. Repairing mojibake is guesswork and would hide the case; the next clean update for the key is accepted (D-027). A test checks rejects against ground truth in both directions: nothing clean is rejected and no corrupted record is missed.

**D-029 · 2026-11-11 · Accent stripping and gender codes are table-driven**
- *Decision:* `config/reference/letters.csv` maps every accented or special Latin letter (U+00C0–U+017F, plus Ø, Æ, Œ, ß, Ł and others with no Unicode decomposition) to ASCII. Standardization maps through it and then drops any remaining combining marks. `config/reference/genders.csv` maps raw gender values to M, F or X; anything unlisted is null.
- *Alternatives:* `unicodedata` in Python and `strip_accents` in DuckDB; hard-coded gender codes.
- *Reason:* Spark has no accent-stripping or NFC function, and letters such as Ø have no Unicode decomposition. A shared table gives all three engines the same result, and mapping before dropping combining marks equals NFC-then-strip, so engines without NFC agree. A test checks the table against Unicode decomposition.

**D-030 · 2026-11-11 · Name comparison forms**
- *Decision:* Each cleaned name yields these forms: the name; without apostrophes and with hyphens as spaces; that with spaces removed; the same three with one leading particle stripped (longest particle first, O' attached); and, for multi-token names, each token of two or more letters that is not a particle word. Name levels are the best result over all pairs of forms. Particles are stripped from either name field, because family-name-first sources put the surname in the first-name field.
- *Alternatives:* Separate rules per edge case (token rule, particle rule, ...).
- *Reason:* One cross-product of forms covers NAME_MIDDLE_IN_FIRST, NAME_CONCATENATED, NAME_COMPOUND_SURNAME and NAME_PARTICLE with a single rule that SQL can express as an exploded join. "Delacruz" vs "Cruz" has no form in common. Both match "De la Cruz", so clustering can still join them through it.

**D-031 · 2026-11-11 · Variant matches use name groups**
- *Decision:* Two names are related when they share a canonical name in `nicknames.csv`: one is a variant of the other, or both are variants of the same canonical (BOB and ROB, MOHAMED and MUHAMMED).
- *Alternatives:* Only direct canonical–variant pairs.
- *Reason:* Two records of one person can each carry a different variant, and transliterations are listed as variants of one canonical spelling, so direct pairs alone would miss MOHAMED vs MOHAMMED.

**D-032 · 2026-11-11 · A January 1 DOB cannot carry Tier 2 or Tier 4** (decided by the project owner)
- *Decision:* `dob_is_jan1` is set on every January 1 DOB. Such a DOB still compares as EXACT and counts in Tier 1 (SSN also exact) and Tier 3, but it cannot be the exact DOB that Tier 2 and Tier 4 rely on.
- *Alternatives:* An informational flag only; comparing two January 1 DOBs as NEAR everywhere.
- *Reason:* Section 3.7 says "kept, flagged weak". Where the DOB is the main evidence (SSN near or missing), a default date is not good enough. Where the SSN is exact, it is.

**D-033 · 2026-11-11 · Tier 4 keeps its reason code when the profile blocks it**
- *Decision:* A pair meeting Tier 4 gets `TIER_4_NO_SSN` in every profile, with `is_match` true only when `tier_4_matches` is set.
- *Alternatives:* `NO_MATCH_INSUFFICIENT` when the profile disallows Tier 4.
- *Reason:* Evaluation and the README need to count how many pairs the conservative profile gives up at Tier 4 (COMMON_NAME_SAME_DOB: "Tier 4 no-match").

**D-034 · 2026-11-11 · Phonetic twins are kept as designed and measured first** (decided by the project owner)
- *Decision:* Twins whose first names match phonetically, by token or by nickname (GIULIA/CHLOE share Double Metaphone KL; JOHN/JANE share JN) and whose SSNs are near reach Tier 2. A scenario test pins this behavior. In the medium run 47 of about 4,800 twin pairs are affected. The rule is revisited in phase 6 with evaluation numbers.
- *Alternatives:* Stricter name evidence in Tier 2; a broader twins blocker.
- *Reason:* Changing a matching rule should be driven by measured precision and recall per edge case, which phase 6 provides.
- *Revisited in phase 6 (kept, decided by the project owner):* On the medium run (conservative, final load) Tier 2 pairs whose first names match only phonetically are 221 same-person and 27 different-person pairs (89% precision, against about 98% for the rest of Tier 2). 22 of the 47 different-person twin pairs merged come through it, and 24 through a shared first-name token (AMIT ARJUN / ARJUN). Overall precision is 0.9916, so the rule stays and the cost is reported in RESULTS.md.

**D-035 · 2026-11-11 · The twins blocker needs both first names** (decided by the project owner)
- *Decision:* `BLOCKED_TWINS` fires only when both records have a first name. A placeholder name such as BABY GIRL is missing, not different.
- *Alternatives:* Read "first NONE" literally, with NONE including missing (section 7).
- *Reason:* Otherwise a newborn's placeholder record gets a cannot-link to its own later named record, contradicting NEWBORN_PLACEHOLDER.

**D-036 · 2026-11-11 · The twins blocker does not fire on an exact SSN** (decided by the project owner)
- *Decision:* `BLOCKED_TWINS` requires SSN NEAR or two different non-null SSNs. Same SSN, DOB and last name with a different first name is Tier 1.
- *Alternatives:* Section 9 as first written, which included SSN EXACT.
- *Reason:* With SSN EXACT included, a legal first-name change is blocked, contradicting LEGAL_FIRST_NAME_CHANGE ("Tier 1"). In the medium run the original rule cannot-linked 882 of 53,171 same-person pairs sharing SSN and DOB (mostly two different first-name distortions on two records), against 10 pairs of different people sharing all three values.

**D-037 · 2026-11-11 · The Python reference covers standardization through blockers** (decided by the project owner)
- *Decision:* Build phase 4 implements sections 5 and 7–9 in Python (`standardization/standardize.py`, `compare.py`, `rules.py`) with unit tests and one scenario test per catalog code. Blocking, clustering and ID reconciliation stay in phase 5.
- *Alternatives:* Standardization only in phase 4.
- *Reason:* One oracle lets the dbt and PySpark implementations be tested against the same expected decisions, and the scenario tests are written once.

**D-038 · 2026-11-11 · Standardization details section 5 leaves open**
- *Decision:*
  - *Names:* any non-ASCII character left after the letter map is removed. A hyphen or apostrophe that is not between two letters or digits becomes a space. A suffix is taken only as the trailing token, and only when another token remains, from the last name first, then the first name.
  - *SSNs:* only purely numeric values (including `.0` and scientific forms) are left-padded to 9 digits. Formatted values must have exactly 9 digits. Scientific notation is expanded only when every digit is present.
  - *DOBs:* accepted formats are ISO (optionally with a time), `MM/DD/YYYY`, `MM/DD/YY` (read as 20YY, then nulled if in the future) and `YYYYMMDD`. Day-first formats are not accepted, because they are ambiguous with US dates.
  - *Null reasons:* every nulled SSN or DOB records why: `MISSING`, `PLACEHOLDER`, `ITIN_OR_TEMPORARY`, `INVALID_AREA`, `INVALID_GROUP`, `INVALID_SERIAL`, `INVALID_LENGTH`, `UNPARSEABLE`, `UNRECOVERABLE`, `YEAR_ONLY`, `INVALID_DATE`, `FUTURE`.
  - *DOB gap:* whole years computed age-style.
- *Alternatives:* Padding every short SSN; a century pivot for two-digit years.
- *Reason:* Padding a formatted SSN that lost a digit would invent a wrong but valid-looking number. The 20YY reading is what section 3.7 describes for DOB_TWO_DIGIT_YEAR. Null reasons make placeholder spikes and source problems visible in metrics.

**D-039 · 2026-11-11 · Portable standardization regexes**
- *Decision:* The Python reference uses only regex features that RE2 (DuckDB) and Java (Spark) handle the same way. Hyphen and apostrophe cleanup is three lookaround-free steps: runs of marks, then a mark after a non-alphanumeric character, then a mark before one. Digits are matched with `[0-9]`, not `\d`, which is Unicode-aware in Python. Raw values are trimmed of ASCII whitespace only. Dates are formatted by hand, not with `strftime`.
- *Alternatives:* Lookbehind in Python with a different expression per engine.
- *Reason:* One expression that reads the same in all three engines makes parity a property of the rules, not of careful translation. RE2 has no lookbehind.

**D-040 · 2026-11-11 · Blocking key values**
- *Decision:* `ssn` is the 9-digit SSN. `dob` is `YYYY-MM-DD`. `ssn_sorted_digits_birth_year` is the SSN's digits sorted ascending, `|`, birth year. `ssn_first5_birth_year` is the first 5 digits, `|`, birth year. `last_phonetic_first_initial_birth_year` is the primary Double Metaphone code of the last name with spaces, hyphens and apostrophes removed, `|`, the first letter of the first name, `|`, birth year. A key needs every field it uses. The cap applies per key value; a candidate pair lists every key that produced it.
- *Alternatives:* Phonetic codes per last-name token; secondary metaphone codes.
- *Reason:* Removing separators makes O'BRIEN, OBRIEN and VAN DER BERG / VANDERBERG share a block. Using one code per record keeps blocks small; the other blocking keys cover the rest.

**D-041 · 2026-11-11 · Record and cluster identity**
- *Decision:* A record key is `SOURCE|ID`; ID reconciliation uses the record uid `SOURCE|ID|LIFE`, so a record re-created after deletion, or a reused source ID, does not inherit the old record's person ID. A cluster id is the smallest record key in the cluster. When an ID's records split evenly, the cluster with the smallest cluster id keeps it. New IDs are issued in cluster id order.
- *Alternatives:* Matching the re-created record back to its old ID through the old record (impossible: the old one is gone); random or hash cluster ids.
- *Reason:* SOURCE_ID_REUSE is a different person under the same source ID, so a source ID alone is not an identity. If it is the same person, matching puts the new life in the same cluster as their other records and it gets their ID anyway. Ordering by record key makes every tie deterministic in both engines.

**D-042 · 2026-11-11 · dbt implementation layout**
- *Decision:* Each load is one `dbt build`. `python -m pipeline match-dbt` passes the load's inputs, its manifest date, and every profile parameter from `config/matching.yaml` as dbt vars. Each profile has its own DuckDB file under `data/warehouse/`. The clustering and reconciliation Python models each return one long table with a `row_type` column, which SQL models split into the output tables. Outputs are written with an explicit `COPY` post-hook, sorted, to `data/matched/dbt/<profile>/load_NNNN/`. The next load reads the previous load's `person_assignment` and `id_state` from there.
- *Alternatives:* dbt-duckdb's `external` materialization, which writes a row of nulls for an empty table; incremental models holding ID history inside the warehouse.
- *Reason:* Plain Parquet files per load are what the PySpark implementation and evaluation will read, so the engines exchange nothing but files. Rerunning a load overwrites its outputs and reads only the previous load, so reruns are idempotent.

**D-043 · 2026-11-11 · Evaluation definitions** (outcome definition, generated results page and chart format decided by the project owner)
- *Decision:*
  - *Scope:* every live record of a load (its `person_assignment`) is scored against the truth in effect at that load. Tags count if the case affected the record at any point in its life.
  - *Pairwise:* precision and recall over all pairs of live records, from the final person IDs. Pair counts come from group sizes, so no pairs are enumerated.
  - *Per record:* a record whose cluster contains any other person's record is a **false merge**. Otherwise one missing any of its true partners is a **missed match**, and the rest are **correct**.
  - *Per edge case:* tagged records with a true partner are split into correct / missed match / false merge. Tagged records without one (the only live record of their person) are reported separately, since only a false merge is possible for them. Pair precision and recall count each pair involving a tagged record once.
  - *Stages:* blocking recall (true pairs compared), precision and recall of the pair decisions before clustering, and same-person counts per reason code, including `SKIPPED_TRANSITIVITY`.
  - *Instability:* over record uids live in two consecutive loads. A changed person ID is corrective, degrading or neutral by the record's own pairwise F1 (1 for a correct singleton), each load against its own truth.
  - *Results page:* `docs/RESULTS.md` is generated from the evaluation outputs of all three profiles, like `DATA_GENERATION.md`. The chart is a small SVG written by `evaluation/chart.py`.
- *Alternatives:* pairwise metrics per edge case only; one strict correct/incorrect figure; matplotlib for the chart; writing RESULTS.md by hand.
- *Reason:* Splitting false merges from missed matches shows which way a rule fails, and for most cases one direction is what matters (twins must be separated, typos joined). Without separating records that have no partner, the 60% of tagged records that are alone would make every case look good. A generated page cannot drift from the code, and the SVG writer needs no dependency and gives byte-identical output.

## 19. Publishing on GitHub

The repository is public on my personal GitHub profile and should read well to someone who spends two minutes on it.

**Repository hygiene**
- Generated data is never committed. `data/` is in `.gitignore`; anyone can regenerate it with `make generate` and a seed.
- Include a `LICENSE` (MIT), `.gitignore`, pinned dependency files, and a `Makefile` or equivalent single-command workflow.
- No secrets, credentials, real names, or real SSNs anywhere, including test fixtures. All data is synthetic.
- Commit in small, logical steps with clear messages, at least one commit per build phase, so the history itself shows how the project was built.

**README (the landing page)**
- Opens with a short summary of the problem, the approach, and the headline results, before any setup instructions.
- A diagram of the pipeline: generator → ingestion → matching (dbt and PySpark) → clustering → ID reconciliation → evaluation.
- A quickstart that runs end to end on the small preset in a few minutes from a fresh clone.
- The key design decisions in brief, each linking to the full reasoning in `docs/`.
- A results table and chart across profiles, and a known limitations section.
- Example queries for pulling edge cases by reason code.

**Continuous integration**
- A GitHub Actions workflow runs the full test suite, including parity and idempotency tests, on the small preset for every push and pull request.
- Show the CI status badge at the top of the README.

**Git workflow with Claude Code**
- Claude Code may create commits locally. It must not create the GitHub repository, push, or change repository settings without asking me first.
