"""Tests for ``synthesis/output_writer.py`` (Epic #17 — Issue #39)."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from synthesis.output_writer import write_retrospective


WEEK = "2026-04-12"


class TestWriteHappyPath:
    def test_writes_file_to_configured_directory(self, tmp_path: Path) -> None:
        out = tmp_path / "retrospectives"
        result = write_retrospective("# Hello\n", out, WEEK)
        assert result == out / f"{WEEK}.md"
        assert result.read_text(encoding="utf-8") == "# Hello\n"

    def test_creates_output_directory_if_missing(self, tmp_path: Path) -> None:
        out = tmp_path / "nested" / "retrospectives"
        # Precondition: neither the retrospectives dir nor its parent exist.
        assert not out.exists()
        result = write_retrospective("content", out, WEEK)
        assert result is not None
        assert out.is_dir()
        assert result.read_text(encoding="utf-8") == "content"

    def test_returns_path_to_written_file(self, tmp_path: Path) -> None:
        out = tmp_path / "retrospectives"
        result = write_retrospective("x", out, WEEK)
        assert isinstance(result, Path)
        assert result.name == f"{WEEK}.md"
        assert result.parent == out

    def test_utf8_content_roundtrips(self, tmp_path: Path) -> None:
        # Non-ASCII — smoke test that the encoding argument is honoured.
        content = "hello — world 🌍\n"
        result = write_retrospective(content, tmp_path, WEEK)
        assert result is not None
        assert result.read_text(encoding="utf-8") == content


class TestIdempotency:
    def test_second_call_returns_none(self, tmp_path: Path) -> None:
        # First call writes.
        write_retrospective("first", tmp_path, WEEK)
        # Second call finds the file and declines to overwrite.
        result = write_retrospective("SECOND", tmp_path, WEEK)
        assert result is None

    def test_second_call_does_not_overwrite(self, tmp_path: Path) -> None:
        first = write_retrospective("first write\n", tmp_path, WEEK)
        assert first is not None
        write_retrospective("second — should be ignored", tmp_path, WEEK)
        # File content is unchanged after the refused second call.
        assert first.read_text(encoding="utf-8") == "first write\n"

    def test_logs_info_on_skip(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        write_retrospective("first", tmp_path, WEEK)
        caplog.clear()
        with caplog.at_level(logging.INFO, logger="synthesis.output_writer"):
            result = write_retrospective("second", tmp_path, WEEK)
        assert result is None
        # At least one INFO message should mention "already exists" / skip.
        assert any(
            "already exists" in rec.getMessage().lower()
            or "skipping" in rec.getMessage().lower()
            for rec in caplog.records
        ), [rec.getMessage() for rec in caplog.records]


class TestAtomicWrite:
    def test_no_tmp_file_remains_on_success(self, tmp_path: Path) -> None:
        result = write_retrospective("content", tmp_path, WEEK)
        assert result is not None
        # The .tmp sibling should be gone after the successful rename.
        tmp_sibling = tmp_path / f"{WEEK}.md.tmp"
        assert not tmp_sibling.exists()

    def test_tmp_file_cleaned_up_on_write_failure(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If os.replace raises, the .tmp file is unlinked.

        We simulate this by monkeypatching ``os.replace`` inside the
        module under test to raise.
        """
        import synthesis.output_writer as ow

        def boom(*args, **kwargs):  # noqa: ANN001, ARG001
            raise OSError("simulated rename failure")

        monkeypatch.setattr(ow.os, "replace", boom)
        with pytest.raises(OSError, match="simulated rename failure"):
            write_retrospective("content", tmp_path, WEEK)
        # The .tmp file must have been cleaned up.
        assert not (tmp_path / f"{WEEK}.md.tmp").exists()
        # And the final path must not have landed.
        assert not (tmp_path / f"{WEEK}.md").exists()
