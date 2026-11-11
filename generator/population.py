"""Build the true population and schedule when each record first appears.

Steps (DESIGN.md section 3.5):

1. **People.** Draw base people (culture, gender, names, SSN, age). For each
   base person, population-level edge cases may create *related* people:
   a twin, a son with the same name, a fraudster using their SSN. Related
   people are real people with their own records, so the matcher has to keep
   them apart.
2. **Schedule.** Every person draws a record count. All record slots are
   shuffled and dealt into loads: the initial load gets exactly
   ``initial_records`` slots, each refresh gets its insert quota.
3. **Dates of birth.** Resolved after scheduling, because newborns must be
   born shortly before their first record, and relatives' DOBs depend on
   each other (twins share one; a son is 20-40 years younger).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta

import numpy as np

from generator import identifiers as ids
from generator.context import Context
from generator.model import Identity, Person
from generator.record_cases import _typo
from generator.rng import chance, pick

DAYS_PER_YEAR = 365.25
# A person is a newborn when their first record is at most this old.
NEWBORN_MAX_AGE_DAYS = 365
SUFFIXES_WITHOUT_PAIR = ("JR", "II", "III", "IV")


@dataclass
class Population:
    persons: list[Person]
    # schedule[load_id] = people whose next record is inserted in that load.
    schedule: list[list[Person]]


@dataclass(frozen=True)
class PopulationCase:
    code: str
    eligible: Callable[[PopulationBuilder, Person], bool]
    create: Callable[[PopulationBuilder, Person, np.random.Generator], list[Person]]


@dataclass
class PopulationBuilder:
    ctx: Context
    rng: np.random.Generator
    persons: list[Person] = field(default_factory=list)

    # ----------------------------------------------------------------- people

    def draw_record_count(self) -> int:
        dist = self.ctx.config.records_per_person
        counts = list(dist.counts)
        probabilities = [dist.counts[c] for c in counts] + [dist.tail_probability]
        index = int(self.rng.choice(len(probabilities), p=np.array(probabilities)))
        if index == len(counts):
            return int(self.rng.integers(dist.tail_min, dist.tail_max + 1))
        return counts[index]

    def _choice(self, mix: dict[str, float]) -> str:
        keys = list(mix)
        return keys[int(self.rng.choice(len(keys), p=np.array([mix[k] for k in keys])))]

    def add(self, person: Person) -> Person:
        self.persons.append(person)
        return person

    def new_person(
        self,
        culture: str | None = None,
        gender: str | None = None,
        newborn: bool | None = None,
    ) -> Person:
        """A random person with no relationship to anyone else."""
        pop = self.ctx.config.population
        pools = self.ctx.pools
        culture = culture or self._choice(pop.culture_mix)
        gender = gender or self._choice(pop.gender_mix)
        if newborn is None:
            newborn = chance(self.rng, pop.newborn_share)

        first = pools.first_name(self.rng, culture, gender)
        middle = None
        if chance(self.rng, pop.middle_name_rate):
            middle = pools.different_first_name(self.rng, culture, gender, {first})
        last = pools.last_name(self.rng, culture)
        compound = None
        if (
            culture == "hispanic"
            and " " not in last
            and chance(self.rng, pop.compound_surname_rate)
        ):
            second = pools.different_last_name(self.rng, culture, last)
            if " " not in second:
                compound = (last, second)
                last = f"{last} {second}"
        suffix = None
        if gender == "M" and not newborn and chance(self.rng, pop.suffix_rate):
            suffix = pick(self.rng, SUFFIXES_WITHOUT_PAIR)

        if newborn:
            dob_rule: tuple = ("newborn",)
        else:
            dob_rule = ("fixed", self._random_dob())

        identity = Identity(
            first=first,
            middle=middle,
            last=last,
            suffix=suffix,
            dob=None,
            ssn=self.ctx.ssns.new(),
            gender=gender,
        )
        return self.add(
            Person(
                person_id=f"T{len(self.persons) + 1:08d}",
                culture=culture,
                identity=identity,
                record_count=self.draw_record_count(),
                is_newborn=newborn,
                compound_surname=compound,
                dob_rule=dob_rule,
            )
        )

    def relative_of(self, base: Person, **identity_fields) -> Person:
        """A new person in the base person's culture with some fields copied."""
        person = self.new_person(
            culture=base.culture,
            gender=identity_fields.pop("gender", None),
            newborn=identity_fields.pop("newborn", False),
        )
        compound = identity_fields.pop("compound_surname", "keep")
        for name, value in identity_fields.items():
            if name == "dob_rule":
                person.dob_rule = value
            else:
                setattr(person.identity, name, value)
        if compound != "keep":
            person.compound_surname = compound
        elif "last" in identity_fields:
            person.compound_surname = base.compound_surname
        return person

    def _random_dob(self) -> date:
        pop = self.ctx.config.population
        start = self.ctx.load_dates[0]
        age_days = self.rng.uniform(pop.min_age_years, pop.max_age_years) * DAYS_PER_YEAR
        dob = start - timedelta(days=int(age_days))
        if chance(self.rng, pop.leap_day_birth_share):
            year = dob.year
            while not (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)):
                year -= 1
            dob = date(year, 2, 29)
        return dob

    def age_at_start(self, person: Person) -> float:
        if person.dob_rule[0] != "fixed":
            return 0.0
        return (self.ctx.load_dates[0] - person.dob_rule[1]).days / DAYS_PER_YEAR


# --------------------------------------------------------------------------- #
# Population-level edge cases
# --------------------------------------------------------------------------- #


def _not_mononym(builder: PopulationBuilder, base: Person) -> bool:
    return not base.is_mononym


def _mononym(builder, base, rng):
    base.is_mononym = True
    base.identity.last = None
    base.identity.middle = None
    base.identity.suffix = None
    base.compound_surname = None
    base.tag("NAME_MONONYM", 0)
    return []


def _siblings(builder: PopulationBuilder, base: Person, rng, count: int, code: str):
    names = {base.identity.first}
    created = []
    for _ in range(count):
        gender = builder._choice(builder.ctx.config.population.gender_mix)
        first = builder.ctx.pools.different_first_name(rng, base.culture, gender, names)
        names.add(first)
        sibling = builder.relative_of(
            base,
            gender=gender,
            newborn=base.is_newborn,
            first=first,
            last=base.identity.last,
            ssn=builder.ctx.ssns.near(base.identity.ssn),
            dob_rule=("same_as", base),
        )
        created.append(sibling)
    for person in (base, *created):
        person.tag(code, 0)
    return created


def _twins(builder, base, rng):
    return _siblings(builder, base, rng, 1, "TWINS")


def _multiples(builder, base, rng):
    return _siblings(builder, base, rng, int(rng.integers(2, 5)), "MULTIPLES")


def _newborn_parent_ssn(builder, base, rng):
    parent = builder.relative_of(
        base,
        gender="F" if rng.random() < 0.6 else "M",
        last=base.identity.last,
        dob_rule=("offset", base, -int(rng.integers(20, 46))),
    )
    base.identity.ssn = parent.identity.ssn
    base.tag("NEWBORN_PARENT_SSN", 0)
    parent.tag("NEWBORN_PARENT_SSN", 0)
    return [parent]


def _jr_sr_eligible(builder, base):
    return (
        base.identity.gender == "M"
        and not base.is_mononym
        and not base.is_newborn
        and builder.age_at_start(base) >= 40
    )


def _jr_sr(builder, base, rng):
    base.identity.suffix = "SR" if rng.random() < 0.5 else None
    max_gap = int(min(40, builder.age_at_start(base) - 1))
    son = builder.relative_of(
        base,
        gender="M",
        first=base.identity.first,
        middle=base.identity.middle,
        last=base.identity.last,
        suffix="JR",
        dob_rule=("offset", base, int(rng.integers(20, max_gap + 1))),
    )
    if rng.random() < 0.3:
        son.identity.ssn = base.identity.ssn  # the son borrows his father's SSN
    base.tag("JR_SR", 0)
    son.tag("JR_SR", 0)
    return [son]


def _common_name_eligible(builder, base):
    pools = builder.ctx.pools
    return (
        not base.is_mononym
        and base.identity.first in pools.common_first_names(base.culture)
        and base.identity.last in pools.common_last_names(base.culture)
    )


def _common_name_same_dob(builder, base, rng):
    stranger = builder.relative_of(
        base,
        gender=base.identity.gender,
        newborn=base.is_newborn,
        first=base.identity.first,
        last=base.identity.last,
        middle=None,
        dob_rule=("same_as", base),
    )
    for person in (base, stranger):
        person.ssn_missing_rate = 0.5
        person.tag("COMMON_NAME_SAME_DOB", 0)
    return [stranger]


def _copy_identity(builder, base, code):
    copy = builder.relative_of(
        base,
        gender=base.identity.gender,
        newborn=base.is_newborn,
        first=base.identity.first,
        last=base.identity.last,
        middle=None,
        ssn=base.identity.ssn,
        dob_rule=("same_as", base),
    )
    copy.is_mononym = base.is_mononym
    base.tag(code, 0)
    copy.tag(code, 0)
    return copy


def _identical_strangers(builder, base, rng):
    return [_copy_identity(builder, base, "IDENTICAL_STRANGERS")]


def _full_identity_copy(builder, base, rng):
    return [_copy_identity(builder, base, "FULL_IDENTITY_COPY")]


def _fraudsters(builder, base, count, code):
    created = []
    for _ in range(count):
        fraudster = builder.new_person(newborn=False)
        fraudster.identity.ssn = base.identity.ssn
        fraudster.tag(code, 0)
        created.append(fraudster)
    base.tag(code, 0)
    return created


def _ssn_shared_fraud(builder, base, rng):
    return _fraudsters(builder, base, 1, "SSN_SHARED_FRAUD")


def _ssn_shared_many(builder, base, rng):
    return _fraudsters(builder, base, int(rng.integers(3, 9)), "SSN_SHARED_MANY")


def _synthetic_identity_eligible(builder, base):
    return len(builder.persons) > 1


def _synthetic_identity(builder, base, rng):
    # Person B is any earlier person; the synthetic identity takes B's name and
    # DOB and the base person's (A's) SSN.
    other = builder.persons[int(rng.integers(len(builder.persons) - 1))]
    synthetic = builder.relative_of(
        other,
        gender=other.identity.gender,
        newborn=other.is_newborn,
        first=other.identity.first,
        last=other.identity.last,
        middle=None,
        ssn=base.identity.ssn,
        dob_rule=("same_as", other),
    )
    synthetic.is_mononym = other.is_mononym
    for person in (base, other, synthetic):
        person.tag("SYNTHETIC_IDENTITY", 0)
    return [synthetic]


def _near_match_attack(builder, base, rng):
    copy = _copy_identity(builder, base, "NEAR_MATCH_ATTACK")
    fields = (
        ["ssn", "dob"]
        + (["first"] if copy.identity.first else [])
        + (["last"] if copy.identity.last else [])
    )
    target = pick(rng, fields)
    if target == "ssn":
        changed = builder.ctx.ssns.distorted(
            base.identity.ssn, lambda s: ids.ssn_substitute_digit(rng, s)
        )
        copy.identity.ssn = changed or builder.ctx.ssns.new()
        if changed:
            builder.ctx.ssns.reserve(changed)
    elif target == "dob":
        copy.dob_rule = ("near", base)
    else:
        setattr(copy.identity, target, _typo(rng, getattr(base.identity, target)))
    return [copy]


POPULATION_CASES: tuple[PopulationCase, ...] = (
    PopulationCase("NAME_MONONYM", lambda b, p: not p.is_newborn, _mononym),
    PopulationCase("TWINS", _not_mononym, _twins),
    PopulationCase("MULTIPLES", _not_mononym, _multiples),
    PopulationCase("NEWBORN_PARENT_SSN", lambda b, p: p.is_newborn, _newborn_parent_ssn),
    PopulationCase("JR_SR", _jr_sr_eligible, _jr_sr),
    PopulationCase("COMMON_NAME_SAME_DOB", _common_name_eligible, _common_name_same_dob),
    PopulationCase("IDENTICAL_STRANGERS", lambda b, p: True, _identical_strangers),
    PopulationCase("SSN_SHARED_FRAUD", lambda b, p: True, _ssn_shared_fraud),
    PopulationCase("SSN_SHARED_MANY", lambda b, p: True, _ssn_shared_many),
    PopulationCase("SYNTHETIC_IDENTITY", _synthetic_identity_eligible, _synthetic_identity),
    PopulationCase("FULL_IDENTITY_COPY", lambda b, p: True, _full_identity_copy),
    PopulationCase("NEAR_MATCH_ATTACK", lambda b, p: True, _near_match_attack),
)


# --------------------------------------------------------------------------- #
# Build, schedule, resolve DOBs
# --------------------------------------------------------------------------- #


def build_population(
    ctx: Context, inserts_per_load: list[int], rng: np.random.Generator
) -> Population:
    """Create people until there are enough record slots, then schedule them."""
    builder = PopulationBuilder(ctx, rng)
    total = sum(inserts_per_load)
    slots = 0
    while slots < total:
        base = builder.new_person()
        created = [base]
        for case in POPULATION_CASES:
            rate = ctx.rate(case.code)
            if rate > 0 and case.eligible(builder, base) and rng.random() < rate:
                created.extend(case.create(builder, base, rng))
        slots += sum(p.record_count for p in created)

    persons = builder.persons
    schedule = _schedule(persons, inserts_per_load, rng)
    _resolve_dobs(ctx, persons, rng)
    return Population(persons=persons, schedule=schedule)


def _schedule(
    persons: list[Person], inserts_per_load: list[int], rng: np.random.Generator
) -> list[list[Person]]:
    slot_owners = [i for i, person in enumerate(persons) for _ in range(person.record_count)]
    order = rng.permutation(len(slot_owners))
    schedule: list[list[Person]] = []
    position = 0
    for load_id, count in enumerate(inserts_per_load):
        owners = [persons[slot_owners[i]] for i in order[position : position + count]]
        for person in owners:
            person.record_loads.append(load_id)
        schedule.append(owners)
        position += count
    return schedule


def _resolve_dobs(ctx: Context, persons: list[Person], rng: np.random.Generator) -> None:
    """Fill in every person's DOB; relatives are always created after their base.

    A newborn is born shortly before the *earliest* first record among everyone
    sharing its DOB (its twins, an identical copy), so none of them appears
    before being born. Afterwards ``is_newborn`` is re-derived from the actual
    age at each person's own first record: a twin first recorded a year later
    is no longer a newborn.
    """
    shared_first_load: dict[int, int] = {}
    for person in persons:
        if person.dob_rule[0] == "same_as" and person.record_loads:
            base_id = id(person.dob_rule[1])
            shared_first_load[base_id] = min(
                shared_first_load.get(base_id, person.first_load), person.first_load
            )

    for person in persons:
        rule = person.dob_rule
        kind = rule[0]
        if kind == "fixed":
            dob = rule[1]
        elif kind == "newborn":
            loads = [*person.record_loads[:1]]
            if id(person) in shared_first_load:
                loads.append(shared_first_load[id(person)])
            first_load = min(loads) if loads else 0
            dob = ctx.load_dates[first_load] - timedelta(days=int(rng.integers(0, 181)))
        elif kind == "same_as":
            dob = rule[1].identity.dob
        elif kind == "offset":
            base_dob = rule[1].identity.dob
            shifted = ids.shift_years(base_dob, rule[2]) or ids.shift_years(
                base_dob.replace(day=28), rule[2]
            )
            dob = shifted + timedelta(days=int(rng.integers(-120, 121)))
        elif kind == "near":
            base_dob = rule[1].identity.dob
            dob = ids.dob_digit_typo(rng, base_dob, ctx.load_dates[0]) or base_dob + timedelta(1)
        else:
            raise ValueError(f"Unknown DOB rule {kind!r}")
        person.identity.dob = dob

    for person in persons:
        if person.record_loads:
            age_days = (ctx.load_dates[person.first_load] - person.identity.dob).days
            person.is_newborn = 0 <= age_days <= NEWBORN_MAX_AGE_DAYS
