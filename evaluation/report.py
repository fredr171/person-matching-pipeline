"""Render docs/RESULTS.md and its chart from evaluation outputs (D-043).

Like docs/DATA_GENERATION.md, the results page is generated, so its numbers
cannot drift from the code: ``python -m pipeline results`` after matching and
evaluating every profile. Only tables and fixed explanations are generated;
interpretation that depends on the numbers belongs in the README.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq

from evaluation.chart import Point, precision_recall_svg
from evaluation.run import matched_loads, read_summary
from generator.docs import CATEGORY_TITLES
from pipeline import config, paths

DOCS_DIR = paths.REPO_ROOT / "docs"
CHART_NAME = "images/precision_recall.svg"
KNOWN_LIMITATIONS = (
    "FULL_IDENTITY_COPY",
    "IDENTICAL_STRANGERS",
    "SSN_REPLACEMENT",
    "FULL_NAME_CHANGE",
    "NAME_MONONYM",
)
DASH = "–"


def _f(value: float | None, digits: int = 3) -> str:
    return DASH if value is None else f"{value:.{digits}f}"


def _pct(part: int, whole: int) -> str:
    return DASH if not whole else f"{100 * part / whole:.1f}%"


def _n(value: int) -> str:
    return f"{value:,}"


def _table(header: list[str], rows: list[list[str]], align: str | None = None) -> list[str]:
    align = align or "l" + "r" * (len(header) - 1)
    marks = {"l": "---", "r": "---:"}
    return [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join(marks[a] for a in align) + " |",
        *("| " + " | ".join(row) + " |" for row in rows),
    ]


@dataclass(frozen=True)
class ProfileResults:
    """Everything the page needs for one profile."""

    name: str
    summaries: list[dict]
    edge_cases: dict[str, dict]
    reason_codes: list[dict]

    @property
    def final(self) -> dict:
        return self.summaries[-1]

    def mean_churn(self) -> float | None:
        rates = [s["instability"]["churn_rate"] for s in self.summaries if s["instability"]]
        return sum(rates) / len(rates) if rates else None

    def changes(self, kind: str) -> int:
        return sum(s["instability"][kind] for s in self.summaries if s["instability"])


def load_profile(engine: str, profile: str, data_dir: Path) -> ProfileResults:
    loads = matched_loads(engine, profile, data_dir)
    if not loads:
        raise FileNotFoundError(f"Profile {profile} has not been matched with {engine}")
    summaries = []
    for load_id in loads:
        try:
            summaries.append(read_summary(engine, profile, load_id, data_dir))
        except FileNotFoundError:
            raise FileNotFoundError(
                f"Load {load_id} of {engine}/{profile} has not been evaluated; run `evaluate`"
            ) from None
    final = paths.evaluation_dir(engine, profile, loads[-1], data_dir)
    edge_cases = {r["code"]: r for r in pq.read_table(final / "edge_cases.parquet").to_pylist()}
    reasons = pq.read_table(final / "reason_codes.parquet").to_pylist()
    return ProfileResults(profile, summaries, edge_cases, reasons)


# --------------------------------------------------------------------------- #
# Sections
# --------------------------------------------------------------------------- #


def _run_section(generation: dict, engine: str, main: ProfileResults) -> list[str]:
    final = main.final
    return [
        "## The run",
        "",
        f"- Generated data: preset **{generation['preset']}**, seed {generation['seed']}, "
        f"{generation['loads']} loads ({generation['load_dates'][0]} to "
        f"{generation['load_dates'][-1]}).",
        f"- Matching engine: {engine}. Final load: {final['load_id']}, "
        f"{_n(final['records'])} live records, {_n(final['pairwise']['actual'])} true "
        "same-person pairs.",
        "- Each profile was matched load by load from load 0 and scored against the truth in "
        "effect at each load.",
        "",
    ]


def _summary_section(profiles: list[ProfileResults]) -> list[str]:
    rows = []
    for p in profiles:
        f = p.final
        outcomes = f["record_outcomes"]
        rows.append(
            [
                f"**{p.name}**" if p is profiles[0] else p.name,
                _f(f["pairwise"]["precision"], 4),
                _f(f["pairwise"]["recall"], 4),
                _f(f["pairwise"]["f1"], 4),
                _n(f["pairwise"]["predicted"] - f["pairwise"]["true_positive"]),
                _n(outcomes.get("FALSE_MERGE", 0)),
                _n(outcomes.get("MISSED_MATCH", 0)),
                _f(None if p.mean_churn() is None else 100 * p.mean_churn(), 2) + "%",
            ]
        )
    return [
        "## Summary by profile",
        "",
        "Final load. Precision and recall are pairwise over all pairs of live records. "
        "A *false-merge record* has at least one record of another person in its cluster; a "
        "*missed-match record* has no wrong records but is missing at least one of its own. "
        "ID churn is the mean share of records, live in two consecutive loads, whose person ID "
        "changed.",
        "",
        *_table(
            [
                "Profile",
                "Precision",
                "Recall",
                "F1",
                "False pairs",
                "False-merge records",
                "Missed-match records",
                "ID churn per load",
            ],
            rows,
        ),
        "",
        f"![Precision and recall by profile]({CHART_NAME})",
        "",
    ]


def _stages_section(profiles: list[ProfileResults]) -> list[str]:
    def row(label, pick):
        return [label, *(pick(p.final) for p in profiles)]

    rows = [
        row("Pairs compared", lambda f: _n(f["stages"]["compared_pairs"])),
        row(
            "Blocking recall (true pairs compared)", lambda f: _f(f["stages"]["blocking_recall"], 4)
        ),
        row("Pair decisions: precision", lambda f: _f(f["stages"]["decisions"]["precision"], 4)),
        row("Pair decisions: recall", lambda f: _f(f["stages"]["decisions"]["recall"], 4)),
        row("Clusters: precision", lambda f: _f(f["pairwise"]["precision"], 4)),
        row("Clusters: recall", lambda f: _f(f["pairwise"]["recall"], 4)),
    ]
    return [
        "## Where pairs are won and lost",
        "",
        "Blocking decides which pairs are compared; pair decisions are the tier and blocker "
        "rules on each compared pair; clustering then adds pairs joined through other records "
        "and removes joins that would put a blocked pair together. Final load.",
        "",
        *_table(["Stage", *(p.name for p in profiles)], rows),
        "",
    ]


def _reason_section(main: ProfileResults) -> list[str]:
    rows = [
        [
            f"`{r['reason_code']}`",
            _n(r["pairs"]),
            _n(r["same_person"]),
            _n(r["different_person"]),
            _pct(r["same_person"], r["pairs"]),
        ]
        for r in sorted(main.reason_codes, key=lambda r: -r["pairs"])
    ]
    return [
        f"## Reason codes against truth ({main.name})",
        "",
        "Every compared pair gets one reason code. A same-person pair under a blocker or "
        "`NO_MATCH_INSUFFICIENT` is a missed match; a different-person pair under a tier that "
        "matches is a false match. `SKIPPED_TRANSITIVITY` counts matches clustering refused.",
        "",
        *_table(["Reason code", "Pairs", "Same person", "Different people", "Same person %"], rows),
        "",
    ]


def _edge_case_rows(main: ProfileResults, codes: list[str]) -> list[list[str]]:
    rows = []
    for code in codes:
        e = main.edge_cases.get(code)
        if e is None:
            rows.append([f"`{code}`", "0", *([DASH] * 4)])
            continue
        partnered = e["with_partner"]
        rows.append(
            [
                f"`{code}`",
                _n(e["records"]),
                f"{_n(partnered)}: {_pct(e['correct'], partnered)} / "
                f"{_pct(e['missed_match'], partnered)} / {_pct(e['false_merge'], partnered)}",
                f"{_n(e['without_partner'])}: "
                f"{_pct(e['without_partner_false_merge'], e['without_partner'])}",
                _f(e["pair_precision"]),
                _f(e["pair_recall"]),
            ]
        )
    return rows


def _baseline_row(main: ProfileResults) -> list[str]:
    """The same columns over every live record."""
    final = main.final
    by = final["record_outcomes_by_partner"]
    partnered = sum(by["with_partner"].values())
    alone = sum(by["without_partner"].values())
    pairs = final["pairwise"]
    return [
        "*all records*",
        _n(final["records"]),
        f"{_n(partnered)}: {_pct(by['with_partner'].get('CORRECT', 0), partnered)} / "
        f"{_pct(by['with_partner'].get('MISSED_MATCH', 0), partnered)} / "
        f"{_pct(by['with_partner'].get('FALSE_MERGE', 0), partnered)}",
        f"{_n(alone)}: {_pct(by['without_partner'].get('FALSE_MERGE', 0), alone)}",
        _f(pairs["precision"]),
        _f(pairs["recall"]),
    ]


def _edge_case_section(main: ProfileResults, catalog) -> list[str]:
    lines = [
        f"## Outcomes per edge case ({main.name})",
        "",
        "Final load. *Records* are live records tagged with the case at any point in their "
        "life. Records **with a true partner** (another live record of the same person) are "
        "split into correct / missed match / false merge. Records **without** one can only go "
        "wrong by a false merge. Pair precision and recall count every pair involving a tagged "
        "record. Expected outcomes are from the [catalog](DATA_GENERATION.md).",
        "",
        "Cases compound and a person's other records carry their own cases, so a tagged "
        "record's outcome also depends on them. Compare each row with the *all records* "
        "baseline at the top of each table rather than with 100%.",
        "",
    ]
    header = [
        "Code",
        "Records",
        "With partner: correct / missed / false merge",
        "Without partner: false merge",
        "Pair precision",
        "Pair recall",
    ]
    for category, title in CATEGORY_TITLES.items():
        cases = [c for c in catalog if c.category == category]
        if not cases:
            continue
        lines += [f"### {title}", ""]
        rows = _edge_case_rows(main, [c.code for c in cases])
        for row, case in zip(rows, cases, strict=True):
            row.append(case.expected_outcome)
        rows.insert(0, [*_baseline_row(main), "Baseline for comparison"])
        lines += [*_table([*header, "Expected"], rows, "lrrrrrl"), ""]
    return lines


def _false_merge_section(profiles: list[ProfileResults], catalog) -> list[str]:
    rows = []
    for case in catalog:
        counts = [
            p.edge_cases.get(case.code, {}).get("false_merge", 0)
            + p.edge_cases.get(case.code, {}).get("without_partner_false_merge", 0)
            for p in profiles
        ]
        if any(counts):
            rows.append([f"`{case.code}`", *(_n(c) for c in counts)])
    return [
        "## False-merge records by profile",
        "",
        "Edge cases with at least one tagged record in a cluster with another person's "
        "records, in any profile. Final load.",
        "",
        *_table(["Code", *(p.name for p in profiles)], rows),
        "",
    ]


def _instability_section(main: ProfileResults) -> list[str]:
    rows = []
    for s in main.summaries:
        i = s["instability"]
        if not i:
            continue
        rows.append(
            [
                str(s["load_id"]),
                _n(i["records_in_both"]),
                _n(i["changed"]),
                _n(i["corrective"]),
                _n(i["degrading"]),
                _n(i["neutral"]),
                _f(None if i["churn_rate"] is None else 100 * i["churn_rate"], 2) + "%",
            ]
        )
    return [
        f"## Entity instability ({main.name})",
        "",
        "For each record live in a load and the one before, did its person ID change? A change "
        "is *corrective* when the record's own pairwise F1 against truth improved, *degrading* "
        "when it fell, and *neutral* otherwise.",
        "",
        *_table(
            [
                "Load",
                "Records in both",
                "ID changed",
                "Corrective",
                "Degrading",
                "Neutral",
                "Churn",
            ],
            rows,
        ),
        "",
    ]


def _limitations_section(main: ProfileResults) -> list[str]:
    return [
        f"## Known limitations, measured ({main.name})",
        "",
        "These cases cannot be solved with five fields (DESIGN.md section 16). Columns as in "
        "the edge case tables.",
        "",
        *_table(
            [
                "Code",
                "Records",
                "With partner: correct / missed / false merge",
                "Without partner: false merge",
                "Pair precision",
                "Pair recall",
            ],
            _edge_case_rows(main, list(KNOWN_LIMITATIONS)),
        ),
        "",
    ]


def render_results(data_dir: Path, engine: str = "dbt") -> tuple[str, str]:
    """Return (RESULTS.md text, chart SVG) for every configured profile."""
    matching = config.load_matching_config()
    order = [
        matching.default_profile,
        *(p for p in matching.profiles if p != matching.default_profile),
    ]
    profiles = [load_profile(engine, name, data_dir) for name in order]
    finals = {p.final["load_id"] for p in profiles}
    if len(finals) != 1:
        raise ValueError(f"Profiles were matched through different loads: {sorted(finals)}")
    generation = json.loads((data_dir / "loads" / "generation.json").read_text(encoding="utf-8"))
    catalog = config.load_generator_config().edge_cases
    main = profiles[0]

    lines = [
        "# Results",
        "",
        "<!-- Generated by `python -m pipeline results` from the evaluation outputs. "
        "Do not edit by hand. -->",
        "",
        "The edge case rates are deliberately exaggerated (D-008), so these numbers describe "
        "behaviour under stress. They are not a forecast of accuracy on production data. "
        "Definitions are in [DESIGN.md section 14](DESIGN.md#14-evaluation) and D-043.",
        "",
        *_run_section(generation, engine, main),
        *_summary_section(profiles),
        *_stages_section(profiles),
        *_reason_section(main),
        *_edge_case_section(main, catalog),
        *_false_merge_section(profiles, catalog),
        *_instability_section(main),
        *_limitations_section(main),
    ]
    chart = precision_recall_svg(
        [
            Point(p.name, p.final["pairwise"]["recall"], p.final["pairwise"]["precision"])
            for p in profiles
        ],
        f"Precision and recall by profile (load {main.final['load_id']})",
    )
    return "\n".join(lines).rstrip() + "\n", chart


def write_results(
    data_dir: Path = paths.DEFAULT_DATA_DIR, engine: str = "dbt", docs_dir: Path = DOCS_DIR
) -> Path:
    text, chart = render_results(data_dir, engine)
    target = docs_dir / "RESULTS.md"
    (docs_dir / CHART_NAME).parent.mkdir(parents=True, exist_ok=True)
    (docs_dir / CHART_NAME).write_text(chart, encoding="utf-8", newline="\n")
    target.write_text(text, encoding="utf-8", newline="\n")
    return target
