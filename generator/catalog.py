"""Where each edge case code is implemented.

The catalog in ``config/generator.yaml`` lists 60 codes. Each one is
implemented in exactly one of four places, matching the unit its rate applies to:

=============  ===========================  ======================================
Kind           Module                       Applied
=============  ===========================  ======================================
population     ``generator.population``     when people are created
timeline       ``generator.timeline``       as dated life events
record         ``generator.record_cases``   when a source record is created
pipeline       ``generator.simulate``       to events and loads
=============  ===========================  ======================================

A test checks that this mapping covers the config catalog exactly.
"""

from __future__ import annotations

from generator.population import POPULATION_CASES
from generator.record_cases import RECORD_CASES
from generator.simulate import PIPELINE_CASES
from generator.timeline import TIMELINE_CASES


def implementations() -> dict[str, str]:
    """Map each implemented edge case code to its kind."""
    kinds: dict[str, str] = {}
    sources = (
        ("population", [c.code for c in POPULATION_CASES]),
        ("timeline", [c.code for c in TIMELINE_CASES]),
        ("record", [c.code for c in RECORD_CASES]),
        ("pipeline", list(PIPELINE_CASES)),
    )
    for kind, codes in sources:
        for code in codes:
            if code in kinds:
                raise ValueError(f"{code} is implemented twice ({kinds[code]} and {kind})")
            kinds[code] = kind
    return kinds
