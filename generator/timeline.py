"""Life and legal events placed on people's identity timelines (DESIGN.md section 3.6).

Each event happens at one refresh load. From then on, new records show the
changed identity, and the person's existing records receive updates carrying
it over the following months. Events stack on the same person: a marriage, a
later divorce and a DOB correction can all happen to one person.

An event is only eligible when it can be observed: the person must already
have a record before the event, so old and new values meet in the data.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import numpy as np

from generator import identifiers as ids
from generator.context import Context
from generator.model import IdentityChange, Person
from generator.population import DAYS_PER_YEAR

Eligible = Callable[[Context, Person], bool]
Schedule = Callable[[Context, Person, np.random.Generator], list[IdentityChange]]


@dataclass(frozen=True)
class TimelineCase:
    code: str
    eligible: Eligible
    schedule: Schedule


def _event_load(ctx: Context, person: Person, rng: np.random.Generator, after: int) -> int:
    """A refresh load strictly after ``after``."""
    return int(rng.integers(after + 1, ctx.last_load + 1))


def _age_at(ctx: Context, person: Person, load_id: int) -> float:
    return (ctx.load_dates[load_id] - person.identity.dob).days / DAYS_PER_YEAR


def _observable(ctx: Context, person: Person) -> bool:
    return bool(person.record_loads) and person.first_load < ctx.last_load


def _named(ctx: Context, person: Person) -> bool:
    return _observable(ctx, person) and not person.is_mononym


def _marriage(ctx, person, rng):
    load = _event_load(ctx, person, rng, person.first_load)
    current = person.identity_at(load).last
    new_last = ctx.pools.different_last_name(rng, person.culture, current)
    return [IdentityChange(load, "MARRIAGE", {"last": new_last})]


def _divorce_eligible(ctx, person):
    marriage = next((c for c in person.changes if c.code == "MARRIAGE"), None)
    return marriage is not None and marriage.load_id < ctx.last_load


def _divorce_restore(ctx, person, rng):
    marriage = next(c for c in person.changes if c.code == "MARRIAGE")
    load = _event_load(ctx, person, rng, marriage.load_id)
    original = person.identity_at(marriage.load_id - 1).last
    return [IdentityChange(load, "DIVORCE_RESTORE", {"last": original})]


def _legal_first_name_change(ctx, person, rng):
    load = _event_load(ctx, person, rng, person.first_load)
    current = person.identity_at(load)
    fields: dict = {}
    gender = current.gender
    if rng.random() < 0.5 and gender in {"M", "F"}:
        gender = {"M": "F", "F": "M"}[gender]
        fields["gender"] = gender
    fields["first"] = ctx.pools.different_first_name(rng, person.culture, gender, {current.first})
    return [IdentityChange(load, "LEGAL_FIRST_NAME_CHANGE", fields)]


def _adoption_eligible(ctx, person):
    return _named(ctx, person) and _age_at(ctx, person, ctx.last_load) < 18


def _adoption(ctx, person, rng):
    load = _event_load(ctx, person, rng, person.first_load)
    current = person.identity_at(load).last
    return [
        IdentityChange(
            load,
            "ADOPTION_NAME_CHANGE",
            {"last": ctx.pools.different_last_name(rng, person.culture, current)},
        )
    ]


def _full_name_change(ctx, person, rng):
    load = _event_load(ctx, person, rng, person.first_load)
    current = person.identity_at(load)
    first = ctx.pools.different_first_name(rng, person.culture, current.gender, {current.first})
    last = ctx.pools.different_last_name(rng, person.culture, current.last)
    return [IdentityChange(load, "FULL_NAME_CHANGE", {"first": first, "last": last})]


def _ssn_replacement_eligible(ctx, person):
    # Needs a record created after the change to show the new SSN.
    return (
        bool(person.record_loads)
        and person.last_load > person.first_load
        and person.identity.ssn is not None
    )


def _ssn_replacement(ctx, person, rng):
    load = int(rng.integers(person.first_load + 1, person.last_load + 1))
    return [IdentityChange(load, "SSN_REPLACEMENT", {"ssn": ctx.ssns.new()}, new_records_only=True)]


def _dob_corrected(ctx, person, rng):
    load = _event_load(ctx, person, rng, person.first_load)
    true_dob = person.identity.dob
    wrong = ids.dob_digit_typo(rng, true_dob, ctx.load_dates[0])
    if wrong is None:
        return []
    person.identity.dob = wrong
    return [IdentityChange(load, "DOB_CORRECTED", {"dob": true_dob})]


def _newborn_placeholder_eligible(ctx, person):
    return person.is_newborn and _observable(ctx, person)


def _newborn_placeholder(ctx, person, rng):
    # Handled when records are created: they start as "Baby Boy/Girl <Last>".
    person.newborn_placeholder = True
    person.tag("NEWBORN_PLACEHOLDER", person.first_load)
    return []


TIMELINE_CASES: tuple[TimelineCase, ...] = (
    TimelineCase("DOB_CORRECTED", _observable, _dob_corrected),
    TimelineCase(
        "MARRIAGE", lambda c, p: _named(c, p) and _age_at(c, p, p.first_load) >= 18, _marriage
    ),
    TimelineCase("DIVORCE_RESTORE", _divorce_eligible, _divorce_restore),
    TimelineCase(
        "LEGAL_FIRST_NAME_CHANGE",
        lambda c, p: _named(c, p) and not p.is_newborn,
        _legal_first_name_change,
    ),
    TimelineCase("ADOPTION_NAME_CHANGE", _adoption_eligible, _adoption),
    TimelineCase("FULL_NAME_CHANGE", _named, _full_name_change),
    TimelineCase("SSN_REPLACEMENT", _ssn_replacement_eligible, _ssn_replacement),
    TimelineCase("NEWBORN_PLACEHOLDER", _newborn_placeholder_eligible, _newborn_placeholder),
)


def schedule_life_events(ctx: Context, persons: list[Person], rng: np.random.Generator) -> None:
    """Place eligible life events on each person's timeline, in catalog order."""
    for person in persons:
        for case in TIMELINE_CASES:
            rate = ctx.rate(case.code)
            if rate <= 0 or not case.eligible(ctx, person) or rng.random() >= rate:
                continue
            changes = case.schedule(ctx, person, rng)
            person.changes.extend(changes)
            # identity_at() walks changes in load order.
            person.changes.sort(key=lambda change: change.load_id)
