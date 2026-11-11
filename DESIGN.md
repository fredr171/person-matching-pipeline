# Person Matching: Design Specification

This document is the source of truth for the project. Code, tests, and the README must agree with it. If an implementation choice conflicts with this spec, update the spec first and record why in the decision log (section 16).

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

```
person-matching/
├── README.md
├── docs/
│   ├── DESIGN.md              # this file
│   ├── DATA_GENERATION.md     # generator and edge case catalog
│   ├── MATCHING_RULES.md      # rules and their defense
│   └── RESULTS.md             # metrics per profile, known limitations
├── config/
│   ├── generator.yaml         # presets, edge case rates, seed
│   ├── matching.yaml          # profiles, thresholds, blocker settings
│   └── reference/             # nicknames.csv, placeholders.csv, particles.csv
├── generator/                 # synthetic data + CDC event generator
├── ingestion/                 # CDC -> current-state table
├── matching_dbt/              # dbt project (DuckDB)
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

Shared Python module, run before either matching implementation. Output: `current_records.parquet` (one row per live source record) and `rejects.parquet`.

1. **Validate** each event against the schema contract. Reject with a reason code (`REJECT_MISSING_KEY`, `REJECT_COLUMN_SHIFT`, `REJECT_ENCODING`, `REJECT_BAD_OP`). Rejections are logged, not queued for review.
2. **Deduplicate** by `event_id`.
3. **Order** by `sequence_no` per `(source_system, source_record_id)`. `event_ts` is informational only.
4. **Apply** events to the prior current state. The highest sequence_no wins. A delete creates a tombstone; any event with a lower sequence_no than the tombstone is ignored. An insert with a higher sequence_no after a delete starts a new record life.
5. **Idempotency:** reprocessing any load, or replaying old loads, must produce the same current state.

## 5. Standardization

Applied identically in both implementations. Raw values are kept alongside cleaned values.

- Unicode NFC normalization, then strip accents; uppercase; trim and collapse whitespace; remove punctuation except hyphens and apostrophes inside names (then compare with and without them).
- Extract suffixes (JR, SR, II, III, IV, in all common formats) from last and first names into `suffix`.
- Strip name particles (DE, DE LA, DEL, VAN, VAN DER, VON, AL, EL, DA, DI, LE, LA, O') into a particle-free comparison form; keep the original too.
- Tokenize names; keep a space-removed form.
- SSN: keep digits only; if numeric artifacts are detected, recover (drop `.0`, expand scientific notation where lossless), left-pad to 9. Null out placeholders, invalid areas (000, 666), group 00, serial 0000, and anything starting with 9.
- DOB: parse common formats; null out placeholders, future dates, year-only values; flag Jan 1 as weak.
- Placeholder names (from `config/reference/placeholders.csv`) become null.
- Gender normalized to M, F, X, or null.

## 6. Blocking

Candidate pairs are generated from records sharing any of these keys (skipping null keys):

| Key | Catches |
|---|---|
| ssn | Tier 1 and 3, SSN conflicts, generational cases |
| dob | Tier 2 and 4, twins |
| sorted digits of ssn + birth year | Transpositions with DOB errors |
| ssn first 5 + dob year | Substitutions in last 4 |
| phonetic(last) + first initial + birth year | Name-led matches with DOB errors |

Blocks larger than a configured cap are skipped and reported in metrics (a spike usually means a new placeholder value). Pairs are deduplicated across keys.

## 7. Field comparison levels

**SSN:** `EXACT`, `NEAR` (one substitution or one adjacent transposition), `MISSING`, `DIFFERENT`.

**DOB:** `EXACT`, `NEAR` (month/day swap, one-digit typo, year ±1 with same month/day, leap-day shift), `MISSING`, `DIFFERENT`. Also computed: absolute gap in years.

**Names** (first and last separately, best result across all forms and tokens):

| Level | Rule |
|---|---|
| STRONG | Exact, nickname/variant table, phonetic (Double Metaphone), Jaro-Winkler ≥ strong threshold, or prefix match where the shorter name is ≥ 6 chars |
| WEAK | Jaro-Winkler between weak and strong thresholds, or initial only |
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

Tier 1 deliberately requires only one strong name: exact SSN and exact DOB together are strong enough to survive marriage, divorce, adoption, and legal first-name changes. Twins (different SSNs) and parent/child cases (different DOBs) cannot reach Tier 1.

## 9. Blockers (cannot-link rules)

Evaluated on every candidate pair. A blocker overrides any tier and creates a **cannot-link** constraint used in clustering.

| Blocker | Condition | Reason code |
|---|---|---|
| Generational | SSN EXACT or NEAR, DOB gap ≥ 12 years | `BLOCKED_GENERATIONAL` |
| Suffix conflict | Both suffixes non-null and different | `BLOCKED_SUFFIX` |
| Twins/multiples | DOB EXACT, last STRONG, first NONE (not a nickname), SSN EXACT or NEAR or both non-null and differing | `BLOCKED_TWINS` |
| SSN conflict | SSN EXACT, DOB DIFFERENT, both names NONE | `BLOCKED_SSN_CONFLICT` |
| Newborn on SSN | One record has a placeholder name and DOB within the last year, other record's DOB ≥ 12 years older | `BLOCKED_GENERATIONAL` |

## 10. Clustering

Pairwise matches are not transitive by default. Clustering is constrained and deterministic:

1. Compute connected components over all match edges (this bounds the work).
2. Within each component, sort match edges by tier ascending, then by `(min record key, max record key)` for a stable order.
3. Greedy union-find: join two clusters only if no cannot-link pair exists between any member of one and any member of the other. Skipped joins are recorded with `SKIPPED_TRANSITIVITY`.

Implementation: standardization, blocking, comparison, tiers, and blockers are SQL in dbt and DataFrame operations in PySpark. The constrained clustering step is sequential by nature, so both implementations call the **same shared Python function** in `clustering/`: a dbt Python model on DuckDB, and `groupBy(component).applyInPandas` in PySpark. This is a deliberate, documented decision: identical logic beats forcing a greedy algorithm into SQL.

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

Users must be able to pull edge cases with simple queries, e.g. all `BLOCKED_TWINS` pairs, all `SKIPPED_TRANSITIVITY` joins, all ID merges this load. Provide example queries in the README.

## 14. Evaluation

Run per load and per profile, reading truth only in `evaluation/`:

- **Pairwise** precision, recall, F1 over record pairs (true same-person pairs vs predicted).
- **Per edge case**: for each edge case code, how many affected records ended up correctly matched or correctly separated.
- **Entity instability**: for each record present in consecutive loads, did its person_id change? Classify each change as *corrective* (the record's pairwise F1 against truth improved), *degrading* (worsened), or *neutral*. Report churn rate and the corrective/degrading split.
- A summary table and a precision/recall chart across the three profiles for `docs/RESULTS.md`.

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

Record each significant decision with date, decision, alternatives considered, and reason. Seed it with: no human review; no-match default; Tier 1 one-strong-name rule; full rematch each load; ID reconciliation rules; shared Python clustering; conservative default profile; exaggerated edge case rates.

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
