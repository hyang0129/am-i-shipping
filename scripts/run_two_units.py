"""Run summarize + expectation extraction for two specific units only.

Usage:
    python scripts/run_two_units.py
"""
from __future__ import annotations

import logging
import sqlite3
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

WEEK_START = "2026-04-14"
TARGET_UNIT_IDS = {"45d36ccb02b9fb09", "46e3bb0455004d17"}  # issues #139 and #163

from am_i_shipping.config_loader import load_config
from am_i_shipping.db import init_github_db, init_expectations_db

from synthesis.summarize import (
    _build_unit_input as summarize_build_input,
    _summarize_unit,
    _store_summary,
)
from synthesis.expectations import (
    _build_unit_input as expectations_build_input,
    _parse_llm_response,
    _store_expectation,
    _load_extracted_unit_ids,
    _EXPECTATIONS_SYSTEM_PROMPT,
    _MAX_OUTPUT_TOKENS,
)
from synthesis.llm_adapter import _get_adapter
from synthesis.calibration import build_few_shot_block


def main() -> None:
    config = load_config()
    synth_config = config.synthesis

    github_db = str(config.data_path / "github.db")
    sessions_db = str(config.data_path / "sessions.db")
    expectations_db = str(config.data_path / "expectations.db")

    init_github_db(github_db)
    init_expectations_db(expectations_db)

    gh_conn = sqlite3.connect(github_db)
    sess_conn = sqlite3.connect(sessions_db)
    exp_conn = sqlite3.connect(expectations_db)

    # --- Stage 1: Summarize ---
    print("\n=== STAGE 1: Summarize ===")
    already_summarized = {
        r[0] for r in gh_conn.execute(
            "SELECT unit_id FROM unit_summaries WHERE week_start = ?", (WEEK_START,)
        ).fetchall()
    }

    for unit_id in sorted(TARGET_UNIT_IDS):
        if unit_id in already_summarized:
            print(f"  {unit_id}: already summarized, skipping")
            continue
        print(f"  {unit_id}: building input...")
        unit_input, input_bytes = summarize_build_input(gh_conn, sess_conn, unit_id, WEEK_START)
        print(f"  {unit_id}: calling LLM ({input_bytes} bytes)...")
        try:
            summary = _summarize_unit(synth_config, unit_input)
            _store_summary(gh_conn, WEEK_START, unit_id, summary, synth_config.summary_model, input_bytes)
            print(f"  {unit_id}: summarized ({len(summary.split())} words)")
        except Exception as exc:
            print(f"  {unit_id}: FAILED — {exc}", file=sys.stderr)

    # --- Stage 2: Extract expectations ---
    print("\n=== STAGE 2: Extract expectations ===")
    adapter = _get_adapter(synth_config)
    try:
        few_shot_block = build_few_shot_block(expectations_db)
    except Exception:
        few_shot_block = ""

    already_extracted = _load_extracted_unit_ids(exp_conn, WEEK_START)

    for unit_id in sorted(TARGET_UNIT_IDS):
        if unit_id in already_extracted:
            print(f"  {unit_id}: already extracted, skipping")
            continue
        print(f"  {unit_id}: building input...")
        unit_input, input_bytes, candidate_idx, skip_reason = expectations_build_input(
            gh_conn, sess_conn, unit_id, WEEK_START
        )
        print(f"  {unit_id}: skip_reason={skip_reason!r}, input_bytes={input_bytes}")

        if skip_reason:
            _store_expectation(
                exp_conn,
                week_start=WEEK_START,
                unit_id=unit_id,
                commitment_point=None,
                expected_scope=None,
                expected_effort=None,
                expected_outcome=None,
                confidence=None,
                model=synth_config.model,
                input_bytes=input_bytes,
                skip_reason=skip_reason,
            )
            print(f"  {unit_id}: stored skip row ({skip_reason})")
            continue

        print(f"  {unit_id}: calling LLM...")
        classifier_input = f"{few_shot_block}\n{unit_input}" if few_shot_block else unit_input
        try:
            result = adapter.call(
                _EXPECTATIONS_SYSTEM_PROMPT,
                classifier_input,
                synth_config.model,
                _MAX_OUTPUT_TOKENS,
            )
            parsed = _parse_llm_response(result.text)
        except Exception as exc:
            print(f"  {unit_id}: LLM FAILED — {exc}", file=sys.stderr)
            _store_expectation(
                exp_conn,
                week_start=WEEK_START,
                unit_id=unit_id,
                commitment_point=None,
                expected_scope=None,
                expected_effort=None,
                expected_outcome=None,
                confidence=None,
                model=synth_config.model,
                input_bytes=input_bytes,
                skip_reason="llm_failed",
            )
            continue

        if not parsed:
            print(f"  {unit_id}: LLM returned unparseable response")
            _store_expectation(
                exp_conn,
                week_start=WEEK_START,
                unit_id=unit_id,
                commitment_point=None,
                expected_scope=None,
                expected_effort=None,
                expected_outcome=None,
                confidence=None,
                model=synth_config.model,
                input_bytes=input_bytes,
                skip_reason="llm_unparseable",
            )
            continue

        _store_expectation(
            exp_conn,
            week_start=WEEK_START,
            unit_id=unit_id,
            commitment_point=parsed.get("commitment_point"),
            expected_scope=parsed.get("expected_scope"),
            expected_effort=parsed.get("expected_effort"),
            expected_outcome=parsed.get("expected_outcome"),
            confidence=parsed.get("confidence"),
            model=synth_config.model,
            input_bytes=input_bytes,
            skip_reason=None,
        )
        print(f"  {unit_id}: extracted — scope={parsed.get('expected_scope')} effort={parsed.get('expected_effort')} confidence={parsed.get('confidence')}")

    gh_conn.close()
    sess_conn.close()
    exp_conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
