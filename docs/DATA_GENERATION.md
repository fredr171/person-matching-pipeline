# Data Generation

<!-- Generated from config/generator.yaml by `python -m pipeline docs`. Do not edit. -->

The generator builds a synthetic population of true people, then emits their records from several source systems as monthly CDC loads. Ground truth (which record belongs to which person, and which edge cases apply) is written to a separate directory that only `evaluation/` and the tests may read.

Default seed: `42`. Load 0 is dated 2025-01-01 and each refresh load is one month later. The same config, preset and seed always produce byte-identical files.

## Presets

| Preset | Initial records | Records per refresh | Refresh loads |
|---|---:|---:|---:|
| small | 1,000 | 40–60 | 12 |
| medium (default) | 100,000 | 2,000–5,000 | 12 |
| large | 5,000,000 | 90,000–110,000 | 12 |

Each refresh is split 70% inserts, 25% updates and 5% deletes. Edge cases add events on top: corrections, migrations, duplicates and replays.

## Population

- **Records per person:** 1: 80%, 2: 12%, 3: 4%, 4: 2%, 5: 1%, 6–15: 1% (expected mean 1.395).
- **Name cultures:** anglo 55%, hispanic 20%, east asian 8%, south asian 7%, arabic 5%, european 5%. Names come from `generator/data/`.
- **Ages:** 1–100 years, plus 2% newborns. 1% are born on Feb 29.
- **Names:** 60% have a middle name; 40% of Hispanic people have a two-part surname; 2% of men have a generational suffix.

## Source systems

Formatting differences between sources are baseline noise, not edge cases. Standardization must handle all of them.

| Source | Share | Names | DOB | SSN | SSN missing | Gender missing |
|---|---:|---|---|---|---:|---:|
| EHR_A | 45% | title | iso | dashed | 5% | 2% |
| CLAIMS_B (family name first for East Asian names) | 35% | upper | us | digits | 2% | 3% |
| LAB_C | 20% | mixed | compact | spaced | 30% | 10% |

DOB formats: iso `YYYY-MM-DD`, us `MM/DD/YYYY`, compact `YYYYMMDD`. SSN formats: dashed `123-45-6789`, digits `123456789`, spaced `123 45 6789`. After a migration a source rotates to the next format in each list.

## Edge case catalog

Rates are **deliberately exaggerated** (D-008) so every case is frequent enough to measure. A rate is the probability that an *eligible* unit gets the case (D-016). The unit is a person, a record, an event or a whole load.

A field carries at most one distortion (D-020), so an SSN never has both a typo and a placeholder. Distortions of different fields stack, and life events stack on the same person.

### Identifier errors and placeholders

| Code | Description | How it is generated | Expected outcome | Unit | Rate |
|---|---|---|---|---|---:|
| `SSN_TYPO_SUB` | One SSN digit substituted. | Replace one random digit of the true SSN with a different digit. | Tier 2 match | record | 3% |
| `SSN_TYPO_TRANSPOSE` | Two adjacent SSN digits transposed. | Swap one pair of adjacent, non-equal digits. | Tier 2 match | record | 2% |
| `SSN_NUMERIC_CORRUPT` | SSN mangled by a numeric type (leading zeros lost, .0 suffix, scientific notation). | Render the SSN as an integer, float string, or scientific notation. | Fixed by standardization, then normal tiers | record | 2% |
| `SSN_PLACEHOLDER` | Placeholder SSN such as 000-00-0000, 123-45-6789, 999-99-9999 or repeated digits. | Replace the SSN with a value from the SSN placeholders. | Treated as missing | record | 2% |
| `SSN_TEMP_ITIN` | Temporary ID or ITIN in the SSN field (starts with 9). | Replace the SSN with a valid-looking 9xx-xx-xxxx number. | Treated as missing | record | 1% |
| `SSN_DELAYED` | Record first arrives with no SSN; a later update adds it. | Insert with blank SSN, then an update in a later load sets the true SSN. | Same source record updates; matching uses new SSN | record | 2% |
| `SSN_CORRECTED` | Wrong SSN later corrected by the source. | Insert with another person's or a random SSN; a later update restores the true SSN. | Rematch reflects corrected value | record | 1% |
| `DOB_TYPO_DIGIT` | One DOB digit wrong. | Change one digit of the date while keeping it a valid date. | Tier 3 match | record | 2% |
| `DOB_MONTH_DAY_SWAP` | Month and day swapped (03/04 vs 04/03). | Swap month and day; eligible only when both are 12 or less and differ. | Tier 3 match | record | 2% |
| `DOB_YEAR_OFF_ONE` | Year off by one, same month and day. | Add or subtract one year. | Tier 3 match | record | 1% |
| `DOB_LEAP_DAY` | Feb 29 recorded as Feb 28 or Mar 1. | Eligible only for people born on Feb 29; shift to an adjacent day. | Tier 3 match | record | 50% |
| `DOB_PLACEHOLDER` | Default DOB such as 1900-01-01. | Replace the DOB with a value from the DOB placeholders. | Treated as missing | record | 1% |
| `DOB_JAN_FIRST` | Jan 1 used when only the birth year is known. | Keep the true year, set month and day to 01-01. | Kept, flagged weak | record | 1% |
| `DOB_TWO_DIGIT_YEAR` | Two-digit year parsed into the wrong century (85 -> 2085). | Write the DOB with a two-digit year that a naive parser reads as a future date. | Future DOB treated as missing | record | 1% |
| `DOB_YEAR_ONLY` | Only a year provided. | Write only the four-digit year. | Treated as missing | record | 0.5% |
| `DOB_CORRECTED` | DOB legally corrected later. | Person's records carry a wrong DOB; in a later load all sources update to the corrected DOB. | Rematch reflects corrected value | person | 0.5% |

### Name variation

| Code | Description | How it is generated | Expected outcome | Unit | Rate |
|---|---|---|---|---|---:|
| `NAME_TYPO` | Misspelling, duplicated or dropped letter. | Apply one substitution, duplication or deletion to a first or last name. | Strong name via fuzzy | record | 4% |
| `NAME_NICKNAME` | Nickname used instead of the formal name (Dave/David, Bob/Robert, Peggy/Margaret). | Eligible only when the first name has a nickname in nicknames.csv; replace it with one. | Strong name via nickname table | record | 15% |
| `NAME_PHONETIC` | Phonetically equivalent spelling (Catherine/Katherine, Smith/Smyth). | Eligible only for names with a curated sound-alike in generator/data/phonetic_variants.csv; swap to it. | Strong name via phonetic | record | 25% |
| `NAME_TRANSLITERATION` | Transliteration variant (Mohammed/Muhammad/Mohamed). | Eligible only for names with transliteration variants in nicknames.csv; swap to another variant. | Strong name via variant table | record | 30% |
| `NAME_SWAPPED` | First and last names reversed on one record. | Swap first_name and last_name. | Swapped level; matches per tiers | record | 1% |
| `NAME_FAMILY_FIRST` | Systematic family-name-first ordering from one source. | Eligible only for East Asian names in a source with family_name_first; write family name into first_name. | Swapped level | record | 50% |
| `NAME_MIDDLE_IN_FIRST` | Middle name included in the first name ("Mary Ann"). | Eligible only for people with a middle name; append it to first_name. | Token match | record | 10% |
| `NAME_CONCATENATED` | Two-part name written without a space (MaryAnn vs Mary Ann). | Eligible only for multi-token names; remove the space. | Space-removed comparison | record | 30% |
| `NAME_INITIAL` | Initial only ("D." vs David). | Replace the first name with its initial, with or without a period. | Weak name | record | 1% |
| `NAME_TRUNCATED` | Name truncated by a field length limit (Christopher -> Christoph). | Eligible only for names of 8+ characters; cut to between 6 and length-1 characters. | Prefix rule (>= 6 chars) | record | 2% |
| `NAME_COMPOUND_SURNAME` | Compound surname recorded in full or in part (Garcia Lopez vs Lopez). | Eligible only for people with a two-part surname; the record keeps only one part (other records keep both). | Token match | record | 30% |
| `NAME_PARTICLE` | Surname particle written differently or dropped (De la Cruz / Delacruz / Cruz). | Eligible only for surnames with a particle; vary spacing or drop the particle. | Particle-stripped comparison | record | 30% |
| `NAME_UNICODE` | Accents present or absent, composed or decomposed Unicode (Jose vs José). | Eligible only for names containing accents; strip accents or emit NFD form. | NFC normalize, strip accents | record | 30% |
| `NAME_SUFFIX_IN_LAST` | Generational suffix inside the last name ("Smith Jr." / "Smith, Jr"). | Eligible only for people with a suffix; append it to last_name in a random format. | Suffix extracted in standardization | record | 50% |
| `NAME_PLACEHOLDER` | Placeholder name (Baby Boy, Infant, Unknown, FNU, LNU, Test User, Asdf). | Replace first and/or last name with a value from the name placeholders. | Treated as missing | record | 1% |
| `NAME_MONONYM` | Person with a single legal name, repeated in both fields or filled with LNU/FNU. | Generate a mononym person; sources write it twice or pad with a placeholder. | Placeholder handling; documented limitation | person | 0.2% |

### Life and legal events

| Code | Description | How it is generated | Expected outcome | Unit | Rate |
|---|---|---|---|---|---:|
| `MARRIAGE` | Last name changes on marriage. | Eligible only for adults; at a timeline point, new records and updates use a new last name. | Tier 1 (one strong name) | person | 3% |
| `DIVORCE_RESTORE` | Last name changes back after divorce. | Eligible only for people with MARRIAGE; later restore the original last name. | Tier 1 | person | 30% |
| `LEGAL_FIRST_NAME_CHANGE` | First name legally changed, often with a gender change. | Change first name; with 50% probability also change gender. | Tier 1 (one strong name) | person | 0.5% |
| `ADOPTION_NAME_CHANGE` | Child's last name changes on adoption. | Eligible only for minors; change last name at a timeline point. | Tier 1 | person | 1% |
| `GENDER_CHANGE_OR_ERROR` | Gender value differs between records. | Write a different or junk gender value on one record. | Ignored as evidence | record | 2% |
| `FULL_NAME_CHANGE` | Both names change; SSN and DOB stay the same. | Change first and last name at the same timeline point. | Known limitation: no match | person | 0.2% |
| `SSN_REPLACEMENT` | SSA issues a new SSN. | Eligible only for people with records in at least two loads; from a timeline point, new records carry a new unique SSN. | Known limitation unless names and DOB carry it | person | 2% |

### Family and look-alike cases

| Code | Description | How it is generated | Expected outcome | Unit | Rate |
|---|---|---|---|---|---:|
| `TWINS` | Twins with same DOB and last name, different first names, near SSNs. | Create a sibling with the same DOB and last name and an SSN differing in the last digits. | BLOCKED_TWINS | person | 2% |
| `MULTIPLES` | Triplets or more. | Create 2-4 siblings as for TWINS. | BLOCKED_TWINS | person | 0.3% |
| `NEWBORN_PARENT_SSN` | Newborn recorded with a parent's SSN. | Eligible only for newborns; a record carries a generated parent's SSN. | BLOCKED_GENERATIONAL | person | 20% |
| `NEWBORN_PLACEHOLDER` | Newborn first recorded as "Baby Boy Smith", later updated with the real name. | Eligible only for newborns; insert with a placeholder first name, update later. | Placeholder name handling | person | 30% |
| `JR_SR` | Father and son with the same name, possibly sharing an SSN. | Create a son with the father's names plus JR/SR suffixes; sometimes the son's record borrows the father's SSN. | BLOCKED_SUFFIX or BLOCKED_GENERATIONAL | person | 1% |
| `COMMON_NAME_SAME_DOB` | Unrelated people with the same common name and DOB. | Eligible only for people whose first and last names are among the most common in their culture; create a second person copying name and DOB, with a different SSN. | No SSN: Tier 4 no-match (conservative) | person | 5% |
| `IDENTICAL_STRANGERS` | Two unrelated people with identical values in all five fields. | Create a second person copying all five fields. | Known limitation | person | 0.1% |

### Fraud

| Code | Description | How it is generated | Expected outcome | Unit | Rate |
|---|---|---|---|---|---:|
| `SSN_SHARED_FRAUD` | A different person uses a victim's SSN with their own name and DOB. | Create a fraudster whose records carry the victim's SSN. | BLOCKED_SSN_CONFLICT | person | 0.5% |
| `SSN_SHARED_MANY` | Several fraudsters share one SSN. | Create 3-8 fraudsters carrying one victim's SSN. | BLOCKED_SSN_CONFLICT; oversized SSN block monitored | person | 0.1% |
| `SYNTHETIC_IDENTITY` | Real SSN combined with another person's name and DOB. | Create an identity from person A's SSN and person B's name and DOB. | BLOCKED_SSN_CONFLICT where detectable | person | 0.2% |
| `FULL_IDENTITY_COPY` | All five fields copied from a victim. | Create a fraudster whose records copy all five fields. | Known limitation | person | 0.1% |
| `NEAR_MATCH_ATTACK` | One character changed from a real identity. | Copy a victim's fields and change one character in one field. | Measured; conservative thresholds limit exposure | person | 0.2% |

### Source system and pipeline

| Code | Description | How it is generated | Expected outcome | Unit | Rate |
|---|---|---|---|---|---:|
| `MIGRATION_REFORMAT` | Source migration changes formats, truncates names, introduces defaults. | In the load, one not-yet-migrated source re-emits all its live records as updates with new formats, field length limits and default values. | Standardization + normal tiers | load | 8% |
| `SOURCE_ID_CHANGE` | Migration assigns new source_record_ids. | In the load, a subset of one source's records is deleted and re-inserted under new IDs. | Matching on attributes, not source IDs | load | 10% |
| `SOURCE_ID_REUSE` | A deleted source ID is reused for a different person. | Eligible only for deleted IDs; a later insert reuses the ID for another person. | Ingestion treats post-delete insert as a new record version; matching decides | event | 20% |
| `DUPLICATE_DELIVERY` | The same event delivered twice. | Emit an exact copy of the event, including event_id. | Deduplicated by event_id | event | 2% |
| `OUT_OF_ORDER` | An older event arrives after a newer one. | Eligible only for updates that change the record; the update's predecessor (older sequence_no, previous values) is delivered one load late. | Ordered by sequence_no | event | 20% |
| `LATE_AFTER_DELETE` | Update with an older sequence_no arrives after a delete. | Eligible only for deleted records; emit a stale update after the delete. | Ignored; record stays deleted | event | 10% |
| `COLUMN_SHIFT` | A batch of rows shifted by one column. | In the load, a contiguous batch of rows has values shifted one column right. | Rejected with reason | load | 20% |
| `ENCODING_CORRUPTION` | Mojibake (e.g. UTF-8 bytes decoded as Latin-1). | Eligible only for events whose names contain non-ASCII characters; write the names as mojibake. | Rejected or flagged, per validation rules | event | 5% |
| `BULK_REPLAY` | Source replays historical events. | In the load, re-emit all events of one earlier load unchanged. | Idempotent; no change | load | 10% |

## Caveats

- **Nicknames and transliterations come from the matcher's own reference table** (`config/reference/nicknames.csv`). Generated variants are therefore always known to the matcher. Real data contains variants no table lists, so measured recall on `NAME_NICKNAME` and `NAME_TRANSLITERATION` is an upper bound (D-021).
- **Phonetic variants** (`generator/data/phonetic_variants.csv`) are checked by a test to share a Double Metaphone code, so each one tests the phonetic rule rather than fuzzy matching.
- **SSNs are valid in format only**: random numbers, not linked to any real person. Generated data is never committed.
