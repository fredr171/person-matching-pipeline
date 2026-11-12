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


def test_every_build_phase_stage_is_built():
    assert cli._PLANNED_COMMANDS == {}


def test_a_planned_stage_exits_with_its_phase(capsys, monkeypatch):
    monkeypatch.setattr(cli, "_PLANNED_COMMANDS", {"future-stage": (9, "Not built yet")})
    assert cli.main(["future-stage"]) == cli.EXIT_NOT_IMPLEMENTED
    assert "build phase 9" in capsys.readouterr().err


def test_compare_engines_reports_missing_runs(tmp_path):
    with pytest.raises(FileNotFoundError, match="Neither engine"):
        cli.main(["compare-engines", "--profile", "balanced", "--data-dir", str(tmp_path)])


def test_generate_writes_loads(tmp_path, capsys):
    assert cli.main(["generate", "--preset", "small", "--data-dir", str(tmp_path)]) == cli.EXIT_OK
    assert "Generated preset=small" in capsys.readouterr().out
    assert (tmp_path / "loads" / "load_0000" / "events.parquet").is_file()
