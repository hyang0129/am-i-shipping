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
    # Epic #17 — Sub-Issue 2/7 (#35): E-1 (commit data) and E-2 (timeline
    # events) collectors. Both default on; flipping to false turns the new
    # collectors off while leaving the rest of the poll cycle unchanged, as
    # a pause lever for the synthesis rollout. NOTE: this pauses *further*
    # writes — existing rows in ``commits`` and ``timeline_events`` are NOT
    # removed by flipping the flag. For a full rollback the operator must
    # also ``DELETE FROM commits; DELETE FROM timeline_events`` on github.db.
    fetch_commits: bool = True
    fetch_timeline: bool = True


@dataclass
class AppSwitchConfig:
    aw_endpoint: str = "http://localhost:5600"
    poll_interval_seconds: int = 30


@dataclass
class DataConfig:
    data_dir: str = "data"


@dataclass
class SynthesisConfig:
    """Weekly synthesis (Phase 2 — Epic #17).

    Populated from the optional ``synthesis:`` section of config.yaml. All
    fields have defaults; a completely missing section yields this dataclass
    with default values. ``anthropic_api_key_env`` names the environment
    variable that will hold the Anthropic API key — presence of the variable
    itself is NOT validated here; that check is deferred to Sub-Issue 6 of
    Epic #17 so that running the collectors without synthesis configured is
    still valid.
    """

    anthropic_api_key_env: str = "ANTHROPIC_API_KEY"
    model: str = "claude-sonnet-4-6"
    summary_model: str = "claude-haiku-4-5"
    output_dir: str = "retrospectives"
    week_start: str = "monday"  # "monday" or "sunday"
    abandonment_days: int = 14
    outlier_sigma: float = 2.0


_VALID_WEEK_STARTS = {"monday", "sunday"}


@dataclass
class Config:
    session: SessionConfig
    github: GitHubConfig
    appswitch: AppSwitchConfig = field(default_factory=AppSwitchConfig)
    data: DataConfig = field(default_factory=DataConfig)
    synthesis: SynthesisConfig = field(default_factory=SynthesisConfig)
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

    @property
    def synthesis_output_path(self) -> Path:
        """Return the resolved synthesis output directory as an absolute Path.

        Mirrors :py:meth:`data_path` for ``synthesis.output_dir``: relative
        paths resolve against the directory that contained ``config.yaml``,
        NOT against ``data_dir``. The old CLI anchored against
        ``data_dir.parent``, which only happened to coincide with the
        config dir for the default ``data_dir = "data"`` layout and broke
        silently for any other layout.
        """
        p = Path(self.synthesis.output_dir)
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
        fetch_commits=bool(
            github_raw.get("fetch_commits", GitHubConfig.fetch_commits)
        ),
        fetch_timeline=bool(
            github_raw.get("fetch_timeline", GitHubConfig.fetch_timeline)
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

    # --- synthesis (optional section — Epic #17 Phase 2) ---
    synthesis_raw = raw.get("synthesis", {}) or {}
    week_start = str(
        synthesis_raw.get("week_start", SynthesisConfig.week_start)
    ).lower()
    if week_start not in _VALID_WEEK_STARTS:
        raise ConfigError(
            "Invalid synthesis.week_start: "
            f"{week_start!r} (expected one of {sorted(_VALID_WEEK_STARTS)!r})"
        )
    synthesis_cfg = SynthesisConfig(
        anthropic_api_key_env=str(
            synthesis_raw.get(
                "anthropic_api_key_env", SynthesisConfig.anthropic_api_key_env
            )
        ),
        model=str(synthesis_raw.get("model", SynthesisConfig.model)),
        summary_model=str(
            synthesis_raw.get("summary_model", SynthesisConfig.summary_model)
        ),
        output_dir=str(
            synthesis_raw.get("output_dir", SynthesisConfig.output_dir)
        ),
        week_start=week_start,
        abandonment_days=int(
            synthesis_raw.get(
                "abandonment_days", SynthesisConfig.abandonment_days
            )
        ),
        outlier_sigma=float(
            synthesis_raw.get(
                "outlier_sigma", SynthesisConfig.outlier_sigma
            )
        ),
    )

    return Config(
        session=session_cfg,
        github=github_cfg,
        appswitch=appswitch_cfg,
        data=data_cfg,
        synthesis=synthesis_cfg,
        _config_dir=Path(config_path).resolve().parent,
    )
