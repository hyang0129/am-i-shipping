"""Loguru logging configuration for am-i-shipping collectors.

Call ``setup_logging()`` once at the top of each entry-point's ``main()``
function.  Individual modules should only do::

    from loguru import logger

and never configure sinks themselves.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Union

from loguru import logger

_LOG_FORMAT = (
    "{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {name}:{function}:{line} | {message}"
)


def setup_logging(log_dir: Union[str, Path, None] = None) -> None:
    """Configure loguru sinks.

    Parameters
    ----------
    log_dir:
        Directory for the rotating log file.  Defaults to ``logs/`` at
        the repository root (detected as two levels up from this file).
    """
    if log_dir is None:
        log_dir = Path(__file__).parent.parent / "logs"
    else:
        log_dir = Path(log_dir)

    log_dir.mkdir(parents=True, exist_ok=True)

    # Remove the default loguru sink (stderr at DEBUG level)
    logger.remove()

    # Rotating file sink — DEBUG and above
    logger.add(
        log_dir / "github_poller.log",
        format=_LOG_FORMAT,
        level="DEBUG",
        rotation="10 MB",
        retention=7,
        encoding="utf-8",
    )

    # stderr sink — ERROR and above only (surfaces in cron/systemd logs)
    logger.add(
        sys.stderr,
        format=_LOG_FORMAT,
        level="ERROR",
    )
