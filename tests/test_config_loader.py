"""Tests for config_loader.py."""

from pathlib import Path

import pytest
import yaml

from config_loader import Config, ConfigError, load_config


def _write_config(tmp_path: Path, data: dict) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(yaml.dump(data), encoding="utf-8")
    return p


# --- Happy path ---

class TestLoadConfigDefaults:
    """config_loader returns correct defaults when optional fields are absent."""

    def test_minimal_valid_config(self, tmp_path):
        cfg_path = _write_config(tmp_path, {
            "session": {"projects_path": "/home/user/.claude/projects"},
            "github": {"repos": ["owner/repo"]},
        })
        cfg = load_config(cfg_path)

        assert cfg.session.projects_path == "/home/user/.claude/projects"
        assert cfg.session.session_gap_minutes == 30
        assert cfg.session.reprompt_threshold == 3
        assert cfg.github.repos == ["owner/repo"]
        assert cfg.github.backfill_days == 90
        assert cfg.appswitch.aw_endpoint == "http://localhost:5600"
        assert cfg.appswitch.poll_interval_seconds == 30
        assert cfg.data.data_dir == "data"

    def test_all_fields_overridden(self, tmp_path):
        cfg_path = _write_config(tmp_path, {
            "session": {
                "projects_path": "/custom/path",
                "session_gap_minutes": 60,
                "reprompt_threshold": 5,
            },
            "github": {
                "repos": ["a/b", "c/d"],
                "backfill_days": 30,
            },
            "appswitch": {
                "aw_endpoint": "http://localhost:9999",
                "poll_interval_seconds": 10,
            },
            "data": {
                "data_dir": "/tmp/custom_data",
            },
        })
        cfg = load_config(cfg_path)

        assert cfg.session.session_gap_minutes == 60
        assert cfg.session.reprompt_threshold == 5
        assert cfg.github.backfill_days == 30
        assert len(cfg.github.repos) == 2
        assert cfg.appswitch.aw_endpoint == "http://localhost:9999"
        assert cfg.appswitch.poll_interval_seconds == 10
        assert cfg.data.data_dir == "/tmp/custom_data"


# --- Error cases ---

class TestLoadConfigErrors:
    """config_loader raises clearly when a required field is missing."""

    def test_missing_file(self, tmp_path):
        with pytest.raises(ConfigError, match="Config file not found"):
            load_config(tmp_path / "nonexistent.yaml")

    def test_empty_file(self, tmp_path):
        cfg_path = tmp_path / "config.yaml"
        cfg_path.write_text("", encoding="utf-8")
        with pytest.raises(ConfigError, match="YAML mapping"):
            load_config(cfg_path)

    def test_missing_session_section(self, tmp_path):
        cfg_path = _write_config(tmp_path, {
            "github": {"repos": ["a/b"]},
        })
        with pytest.raises(ConfigError, match="session"):
            load_config(cfg_path)

    def test_missing_projects_path(self, tmp_path):
        cfg_path = _write_config(tmp_path, {
            "session": {"session_gap_minutes": 30},
            "github": {"repos": ["a/b"]},
        })
        with pytest.raises(ConfigError, match="projects_path"):
            load_config(cfg_path)

    def test_empty_projects_path(self, tmp_path):
        cfg_path = _write_config(tmp_path, {
            "session": {"projects_path": ""},
            "github": {"repos": ["a/b"]},
        })
        with pytest.raises(ConfigError, match="projects_path"):
            load_config(cfg_path)

    def test_missing_github_section(self, tmp_path):
        cfg_path = _write_config(tmp_path, {
            "session": {"projects_path": "/path"},
        })
        with pytest.raises(ConfigError, match="github"):
            load_config(cfg_path)

    def test_empty_repos_list(self, tmp_path):
        cfg_path = _write_config(tmp_path, {
            "session": {"projects_path": "/path"},
            "github": {"repos": []},
        })
        with pytest.raises(ConfigError, match="repos"):
            load_config(cfg_path)

    def test_missing_repos_field(self, tmp_path):
        cfg_path = _write_config(tmp_path, {
            "session": {"projects_path": "/path"},
            "github": {"backfill_days": 30},
        })
        with pytest.raises(ConfigError, match="repos"):
            load_config(cfg_path)
