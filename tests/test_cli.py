"""The CLI entry point behind every Makefile target (D-010)."""

from __future__ import annotations

import pytest

from pipeline import cli


def test_check_config_succeeds(capsys):
    assert cli.main(["check-config"]) == cli.EXIT_OK
    output = capsys.readouterr().out
    assert "generator.yaml  OK" in output
    assert "matching.yaml   OK" in output
    assert "reference/      OK" in output


@pytest.mark.parametrize(
    ("argv", "phase"),
    [
        (["generate", "--preset", "small"], 2),
        (["ingest"], 3),
        (["match-dbt"], 5),
        (["evaluate"], 6),
        (["match-spark", "--profile", "balanced"], 7),
    ],
)
def test_unbuilt_stages_exit_with_their_phase(capsys, argv, phase):
    assert cli.main(argv) == cli.EXIT_NOT_IMPLEMENTED
    assert f"build phase {phase}" in capsys.readouterr().err
