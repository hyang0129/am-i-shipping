"""Load and validate config.yaml, exposing a typed config object."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import yaml


class ConfigError(Exception):
    """Raised when config.yaml is missing or has invalid/missing required fields."""


@dataclass
class SessionLimiterConfig:
    max_files_per_run: int = 200
    inter_file_delay_seconds: float = 0.05


@dataclass
class SessionConfig:
    projects_path: str
    session_gap_minutes: int = 30
    reprompt_threshold: int = 3
    limiter: SessionLimiterConfig = field(default_factory=SessionLimiterConfig)


@dataclass
class GitHubLimiterConfig:
    inter_request_delay_seconds: float = 1.0
    max_items_per_repo: int = 500
    process_nice_increment: int = 10
    max_calls_per_hour: int = 2500


@dataclass
class GitHubConfig:
    repos: List[str]
    backfill_days: int = 90
    limiter: GitHubLimiterConfig = field(default_factory=GitHubLimiterConfig)


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
    _config_dir: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent.parent, repr=False
    )

    @property
    def data_path(self) -> Path:
        """Return the resolved data directory as an absolute Path.

        Relative paths are resolved from the directory that contained the
        config.yaml file passed to ``load_config``.
        """
        p = Path(self.data.data_dir)
        if not p.is_absolute():
            p = self._config_dir / p
        return p


def load_config(config_path: str | Path | None = None) -> Config:
    """Load config.yaml from *config_path* (default: config.yaml in repo root).

    Raises ``ConfigError`` on missing file, missing required fields, or
    invalid types.
    """
    if config_path is None:
        config_path = Path(__file__).resolve().parent.parent / "config.yaml"
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
    session_limiter_raw = session_raw.get("limiter", {}) or {}
    session_limiter = SessionLimiterConfig(
        max_files_per_run=int(
            session_limiter_raw.get(
                "max_files_per_run", SessionLimiterConfig.max_files_per_run
            )
        ),
        inter_file_delay_seconds=float(
            session_limiter_raw.get(
                "inter_file_delay_seconds",
                SessionLimiterConfig.inter_file_delay_seconds,
            )
        ),
    )
    session_cfg = SessionConfig(
        projects_path=str(projects_path),
        session_gap_minutes=int(
            session_raw.get("session_gap_minutes", SessionConfig.session_gap_minutes)
        ),
        reprompt_threshold=int(
            session_raw.get("reprompt_threshold", SessionConfig.reprompt_threshold)
        ),
        limiter=session_limiter,
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
    github_limiter_raw = github_raw.get("limiter", {}) or {}
    github_limiter = GitHubLimiterConfig(
        inter_request_delay_seconds=float(
            github_limiter_raw.get(
                "inter_request_delay_seconds",
                GitHubLimiterConfig.inter_request_delay_seconds,
            )
        ),
        max_items_per_repo=int(
            github_limiter_raw.get(
                "max_items_per_repo", GitHubLimiterConfig.max_items_per_repo
            )
        ),
        process_nice_increment=int(
            github_limiter_raw.get(
                "process_nice_increment",
                GitHubLimiterConfig.process_nice_increment,
            )
        ),
        max_calls_per_hour=int(
            github_limiter_raw.get(
                "max_calls_per_hour", GitHubLimiterConfig.max_calls_per_hour
            )
        ),
    )
    github_cfg = GitHubConfig(
        repos=[str(r) for r in repos],
        backfill_days=int(
            github_raw.get("backfill_days", GitHubConfig.backfill_days)
        ),
        limiter=github_limiter,
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
        _config_dir=Path(config_path).resolve().parent,
    )
