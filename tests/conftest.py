"""Shared test configuration.

Adds the repository root to sys.path so that modules like config_loader,
health_writer, health_check, and init_db can be imported without
pip-installing the project.
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
