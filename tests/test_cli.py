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
        (["match-spark", "--profile", "balanced"], 7),
    ],
)
def test_unbuilt_stages_exit_with_their_phase(capsys, argv, phase):
    assert cli.main(argv) == cli.EXIT_NOT_IMPLEMENTED
    assert f"build phase {phase}" in capsys.readouterr().err


def test_generate_writes_loads(tmp_path, capsys):
    assert cli.main(["generate", "--preset", "small", "--data-dir", str(tmp_path)]) == cli.EXIT_OK
    assert "Generated preset=small" in capsys.readouterr().out
    assert (tmp_path / "loads" / "load_0000" / "events.parquet").is_file()
