"""S-3 retrospective output writer (Epic #17 — Issue #39).

Writes the rendered weekly retrospective Markdown to
``retrospectives/<week_start>.md`` with two invariants from the epic ADR:

* **Decision 2 — Idempotency by refuse-to-overwrite.** If the output
  file already exists, the writer logs INFO and returns ``None`` without
  touching disk. Re-running ``am-synthesize --week <same-week>`` is a
  no-op — the prior file (which may contain the user's hand-written
  answers under the Clarifying Questions section) is never stomped.
* **Atomic write.** Bytes land in ``<path>.tmp`` first, then a single
  ``os.rename`` promotes them into place. Readers never see a partial
  file even if the writer is killed mid-``write()``.

The writer is intentionally pure I/O: prompt assembly + LLM call live in
:mod:`synthesis.weekly`. Keeping the two concerns apart lets
``--dry-run`` exercise the prompt path without going near the output
file at all.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional, Union


logger = logging.getLogger(__name__)


def write_retrospective(
    content: str,
    output_dir: Union[str, Path],
    week_start: str,
) -> Optional[Path]:
    """Atomically write *content* to ``<output_dir>/<week_start>.md``.

    Parameters
    ----------
    content:
        The rendered Markdown. Written as UTF-8. No trailing newline is
        added — the caller owns the final byte.
    output_dir:
        Directory to write into. Created (with parents) if missing.
    week_start:
        ``YYYY-MM-DD`` anchor. Used verbatim as the filename stem.

    Returns
    -------
    ``Path`` to the file written, or ``None`` if the file already existed
    and the call was a refuse-to-overwrite no-op.

    Idempotency
    -----------
    When the output path exists the function logs INFO and returns
    ``None`` without reading the existing file, writing the tmp file, or
    touching ``output_dir`` in any way. This matches ADR Decision 2:
    the synthesis call is downstream of this check in
    :func:`synthesis.weekly.run_synthesis`, so "file exists" also skips
    the API call.

    Atomicity
    ---------
    The write goes to ``<path>.tmp`` and is then renamed to ``<path>``.
    On POSIX ``os.rename`` is atomic within the same filesystem, so
    readers observe either the old state (nothing) or the new state
    (complete file) — never a half-written file. If something goes
    wrong between the tmp write and the rename, the tmp file is
    unlinked so a later run is not confused by stray ``.tmp`` droppings.
    """
    out_dir = Path(output_dir)
    output_path = out_dir / f"{week_start}.md"

    # --- Decision 2: refuse to overwrite -------------------------------
    if output_path.exists():
        logger.info(
            "Retrospective already exists at %s; skipping write (idempotent)",
            output_path,
        )
        return None

    # --- ensure the output directory exists ----------------------------
    # We do this AFTER the exists check so a pre-existing file under an
    # already-existing directory is the cheap path (no mkdir syscall).
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- atomic write via .tmp + rename --------------------------------
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    try:
        # ``open`` in text mode with an explicit UTF-8 encoding avoids
        # the platform-default encoding mismatch that bites us on
        # Windows CI runners (cp1252 by default).
        with open(tmp_path, "w", encoding="utf-8", newline="") as f:
            f.write(content)
        os.replace(tmp_path, output_path)
    except Exception:
        # Best-effort cleanup — we do not want to leave ``.tmp``
        # droppings that could confuse a later run into thinking it
        # crashed. Swallow cleanup failures so the original exception
        # is what propagates.
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        raise

    logger.info("Wrote retrospective to %s", output_path)
    return output_path


__all__ = ["write_retrospective"]
