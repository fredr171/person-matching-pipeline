# Matching Rules

This document explains each matching rule, with a worked example and the reason for it. The
formal rules are in [DESIGN.md sections 5–9](DESIGN.md#5-standardization). The Python
reference implementation is in `standardization/`. Every example below is a scenario test
in `tests/test_scenarios.py`, and the dbt and PySpark implementations must reproduce them.

Blocking, clustering and ID reconciliation are added in build phase 5.

**The principle:** one imperfect field per automatic match. A pair the rules cannot decide is
a no-match with a reason code, never a guess. The default profile is conservative, because in
healthcare a false merge can put one patient's allergies on another's chart, while a missed
match only leaves a duplicate.

## 1. Standardization

Each record has five fields. Standardization makes them comparable without inventing anything:
a value that cannot be read reliably becomes null, and the reason is recorded.

| Field | What happens | Example |
|---|---|---|
| Names | Accents removed through a shared letter table, uppercased, punctuation removed except hyphens and apostrophes inside a name | `José` → `JOSE`, `Søren` → `SOREN`, `O’Brien` → `O'BRIEN` |
| Suffix | A trailing JR, SR, II, III or IV, in any spelling, moves to its own field | `Miller, Jr.` → `MILLER` + `JR` |
| Placeholder names | Values such as BABY BOY, FNU, LNU, UNKNOWN or ASDF become null and are flagged | `Baby Girl` → null |
| SSN | Digits only. Numbers mangled by spreadsheets are repaired. Placeholders, invalid ranges and ITINs become null | `1.23456780E+07` → `012345678`; `123-45-6789` → null |
| DOB | ISO, US and compact formats are parsed. Placeholders, year-only values, impossible dates and future dates become null. January 1 is flagged as weak | `05/06/80` → 2080 → null (future) |
| Gender | M, F, X, or null | `U`, `?` → null |

Why null rather than repair? A wrong value that looks valid is worse than a missing one. Missing
fields lower a pair's tier; a wrong field can create a match. This is also why SSNs with
separators are not padded: `234-56-780` lost a digit, and padding it would produce a real-looking
but wrong number.

Accent stripping is driven by a table (`config/reference/letters.csv`) rather than each engine's
own function, because Spark has none and letters such as Ø, Æ and ß have no Unicode decomposition
(D-029).

## 2. Comparing fields

**SSN:** EXACT, NEAR (one digit substituted, or two adjacent digits swapped), MISSING, or DIFFERENT.

**DOB:** EXACT, NEAR, MISSING, or DIFFERENT. NEAR covers the four common clerical errors:

| Error | Example |
|---|---|
| Month and day swapped | 1980-03-04 vs 1980-04-03 |
| One digit wrong | 1980-05-06 vs 1980-05-08 |
| Year off by one | 1980-05-06 vs 1981-05-06 |
| Leap day shifted | 1984-02-29 vs 1984-02-28 or 1984-03-01 |

**Names:** STRONG, WEAK, or NONE. Each name is expanded into comparison forms (D-030), and the
best result over every pair of forms counts:

| Form | Catches | Example |
|---|---|---|
| The name | Exact match | CATHERINE = CATHERINE |
| Without apostrophes, hyphens as spaces | Punctuation differences | O'BRIEN = OBRIEN |
| Spaces removed | Concatenation | MARY ANN = MARYANN |
| Leading particle stripped | Particles | DE LA CRUZ = CRUZ |
| Each token | Middle names, compound surnames | GARCIA LOPEZ ∋ LOPEZ |

Two forms are STRONG if they are equal, in the same group of the name variant table (DAVE/DAVID,
BOB/ROB, MOHAMED/MUHAMMED), share a Double Metaphone code (SMITH/SMYTH), score at or above the
profile's Jaro-Winkler threshold (0.95 in conservative), or one is a prefix of the other at least
6 letters long (MAXIMI/MAXIMILIAN, a migration truncation). They are WEAK for a lower Jaro-Winkler
score or an initial (C. vs CATHERINE). A single letter is never STRONG.

Two refinements:
- **Swapped names.** If first-to-last and last-to-first are both STRONG, the pair is treated as
  two strong names and flagged. This catches a source that writes the family name first.
- **Gender-ambiguous nicknames.** CHRIS matching CHRISTINA through the variant table counts only
  as WEAK when the records' genders disagree, because CHRIS is also CHRISTOPHER.

Gender is never positive evidence: agreeing or disagreeing on gender changes no decision.

## 3. Match tiers

| Tier | SSN | DOB | Names | Example |
|---|---|---|---|---|
| 1 | EXACT | EXACT | at least one STRONG | CATHERINE MILLER marries and becomes CATHERINE OKAFOR |
| 2 | NEAR | EXACT | both STRONG | SSN typo 234-56-7801 vs 234-56-7811 |
| 3 | EXACT | NEAR | both STRONG | DOB typo 1980-05-06 vs 1980-05-08 |
| 4 | MISSING | EXACT | both STRONG | No SSN on one side; a match only in the aggressive profile |

Tier 1 needs only one strong name, because SSN and DOB agreeing exactly is strong enough to
survive a marriage, divorce, adoption or legal first-name change. Tiers 2 and 3 already have one
imperfect field, so both names must be strong. Two imperfect fields are never an automatic match:
an SSN typo combined with a DOB typo is `NO_MATCH_INSUFFICIENT`.

A January 1 DOB is often a default for "only the year is known". It still counts in Tier 1 and
Tier 3, but not in Tier 2 or Tier 4, where the DOB is the main evidence (D-032).

Tier 4 (no SSN) is off in the conservative and balanced profiles. Two unrelated JAMES SMITHs born
on the same day are far more common than intuition suggests (COMMON_NAME_SAME_DOB). Such pairs
keep the reason `TIER_4_NO_SSN`, so the cost of the conservative choice can be measured (D-033).

## 4. Blockers

A blocker overrides any tier and records a cannot-link, so clustering will never join the two
records, even through other records.

| Blocker | Condition | Example |
|---|---|---|
| `BLOCKED_GENERATIONAL` | SSN EXACT or NEAR, DOBs at least 12 years apart | Father and son on one SSN; a newborn recorded with a parent's SSN |
| `BLOCKED_SUFFIX` | Both suffixes present and different | JOHN SMITH SR vs JOHN SMITH JR |
| `BLOCKED_TWINS` | Same DOB, last STRONG, both first names present and NONE, SSNs near or different | EMMA and OLIVIA MILLER, born 2010-02-03 |
| `BLOCKED_SSN_CONFLICT` | Same SSN, different DOB, both names NONE | Someone else using a victim's SSN |
| `BLOCKED_GENERATIONAL` (newborn) | A placeholder-named record born within the last year, against a record at least 12 years older | BABY BOY SMITH vs JOHN SMITH, with no SSN shared |

The twins blocker needs both first names, so a newborn's BABY GIRL record still matches its own
later named record (D-035). It does not fire on an exact SSN: same SSN, DOB and last name with a
new first name is a legal name change, which is Tier 1 (D-036).

## 5. Known weaknesses

These are generated deliberately and measured in [RESULTS.md](RESULTS.md):

- **Twins with similar first names.** Twins whose SSNs are near and whose first names match
  phonetically, by token or by nickname reach Tier 2. Examples: JOHN/JANE (Double Metaphone JN),
  GIULIA/CHLOE (KL), AMIT ARJUN/ARJUN. In the medium run this affects 47 of about 4,800 twin
  pairs. It is kept on purpose until evaluation measures the trade-off (D-034).
- **Identical values.** Two people with identical five fields, or a full copy of someone's
  identity, are indistinguishable.
- **Full name change, or a new SSN together with a name change.** No field is left to carry the match.
- **Mononyms.** Without an SSN, one strong name is not enough for any tier.
