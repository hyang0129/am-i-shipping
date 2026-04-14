"""Load and validate config.yaml, exposing a typed config object."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import yaml


class ConfigError(Exception):
    """Raised when config.yaml is missing or has invalid/missing required fields."""


@dataclass
class SessionConfig:
    projects_path: str
    session_gap_minutes: int = 30
    reprompt_threshold: int = 3


@dataclass
class GitHubConfig:
    repos: List[str]
    backfill_days: int = 90


@dataclass
class AppSwitchConfig:
    aw_endpoint: str = "http://localhost:5600"
    poll_interval_seconds: int = 30


@dataclass
class DataConfig:
    data_dir: str = "data"


@dataclass
class Config:
    session: SessionConfig
    github: GitHubConfig
    appswitch: AppSwitchConfig = field(default_factory=AppSwitchConfig)
    data: DataConfig = field(default_factory=DataConfig)

    @property
    def data_path(self) -> Path:
        """Return the resolved data directory as an absolute Path."""
        p = Path(self.data.data_dir)
        if not p.is_absolute():
            # Resolve relative to the repo root (directory containing config.yaml)
            p = Path(__file__).resolve().parent / p
        return p


def load_config(config_path: str | Path | None = None) -> Config:
    """Load config.yaml from *config_path* (default: config.yaml next to this file).

    Raises ``ConfigError`` on missing file, missing required fields, or
    invalid types.
    """
    if config_path is None:
        config_path = Path(__file__).resolve().parent / "config.yaml"
    else:
        config_path = Path(config_path)

    if not config_path.exists():
        raise ConfigError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ConfigError("config.yaml must be a YAML mapping at the top level")

    # --- session (required section) ---
    session_raw = raw.get("session")
    if session_raw is None:
        raise ConfigError("Missing required config section: session")

    projects_path = session_raw.get("projects_path")
    if not projects_path:
        raise ConfigError(
            "Missing required field: session.projects_path "
            "(path to ~/.claude/projects)"
        )
    session_cfg = SessionConfig(
        projects_path=str(projects_path),
        session_gap_minutes=int(
            session_raw.get("session_gap_minutes", SessionConfig.session_gap_minutes)
        ),
        reprompt_threshold=int(
            session_raw.get("reprompt_threshold", SessionConfig.reprompt_threshold)
        ),
    )

    # --- github (required section) ---
    github_raw = raw.get("github")
    if github_raw is None:
        raise ConfigError("Missing required config section: github")

    repos = github_raw.get("repos")
    if not repos or not isinstance(repos, list) or len(repos) == 0:
        raise ConfigError(
            "Missing required field: github.repos "
            "(list of owner/repo strings to poll)"
        )
    github_cfg = GitHubConfig(
        repos=[str(r) for r in repos],
        backfill_days=int(
            github_raw.get("backfill_days", GitHubConfig.backfill_days)
        ),
    )

    # --- appswitch (optional section — defaults are fine) ---
    appswitch_raw = raw.get("appswitch", {}) or {}
    appswitch_cfg = AppSwitchConfig(
        aw_endpoint=str(
            appswitch_raw.get("aw_endpoint", AppSwitchConfig.aw_endpoint)
        ),
        poll_interval_seconds=int(
            appswitch_raw.get(
                "poll_interval_seconds", AppSwitchConfig.poll_interval_seconds
            )
        ),
    )

    # --- data (optional section — defaults are fine) ---
    data_raw = raw.get("data", {}) or {}
    data_cfg = DataConfig(
        data_dir=str(data_raw.get("data_dir", DataConfig.data_dir)),
    )

    return Config(
        session=session_cfg,
        github=github_cfg,
        appswitch=appswitch_cfg,
        data=data_cfg,
    )
