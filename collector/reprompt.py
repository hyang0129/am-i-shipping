"""Re-prompt detection for Claude Code sessions.

Identifies re-prompt sequences: consecutive human turns where the user
is rephrasing the same intent because Claude's response didn't meet
expectations. Sets bail_out flag when rephrase_count exceeds threshold.

No external API calls — runs fully offline.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple


def _is_human_text_turn(msg: Dict[str, Any]) -> bool:
    """Check if a message is a human text turn (not a tool result)."""
    if msg.get("role") != "user":
        return False
    content = msg.get("content", "")
    if isinstance(content, str):
        return bool(content.strip())
    if isinstance(content, list):
        # Check if there are any non-tool-result blocks
        for block in content:
            if isinstance(block, dict):
                if block.get("type") == "tool_result":
                    continue
                # Has non-tool-result content
                return True
            if isinstance(block, str):
                return True
        return False
    return False


def _extract_text(content: Any) -> str:
    """Extract plain text from message content for comparison."""
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict):
                if block.get("type") == "text":
                    parts.append(block.get("text", ""))
        return " ".join(parts).strip()
    return ""


def _has_tool_use(content: Any) -> bool:
    """Check if assistant message content contains tool_use blocks."""
    if isinstance(content, list):
        return any(
            isinstance(b, dict) and b.get("type") == "tool_use"
            for b in content
        )
    return False


def _is_tool_result_turn(msg: Dict[str, Any]) -> bool:
    """Check if a user message is purely tool results."""
    if msg.get("role") != "user":
        return False
    content = msg.get("content", "")
    if isinstance(content, list):
        return all(
            isinstance(b, dict) and b.get("type") == "tool_result"
            for b in content
            if isinstance(b, dict)
        )
    return False


def detect_reprompts(
    messages: List[Dict[str, Any]],
    threshold: int = 3,
) -> Tuple[int, bool]:
    """Detect re-prompt sequences in a session's message list.

    A re-prompt sequence occurs when the user sends multiple text turns
    rephrasing the same intent because the assistant's text-only
    responses didn't satisfy them. Tool interactions (tool_use followed
    by tool_result) between two user text turns indicate the assistant
    was actively working, which breaks the reprompt chain.

    Heuristic: count consecutive user-text -> assistant-text-only ->
    user-text patterns where the assistant did NOT invoke any tools
    between the two user text turns. Each additional user text turn
    in such a streak increments rephrase_count.

    Parameters
    ----------
    messages:
        List of {"role": "user"|"assistant", "content": ...} dicts,
        in chronological order.
    threshold:
        rephrase_count at or above which bail_out is set to True.

    Returns
    -------
    (rephrase_count, bail_out)
    """
    rephrase_count = 0
    consecutive_human_text_count = 0
    # Track whether any tool use happened since the last user text turn
    tool_used_since_last_human_text = False

    for msg in messages:
        role = msg.get("role", "")
        content = msg.get("content", "")

        if role == "assistant":
            if _has_tool_use(content):
                tool_used_since_last_human_text = True
            continue

        if role == "user":
            # Tool result turns — mark that tools were used but don't
            # count as a human text turn
            if _is_tool_result_turn(msg):
                tool_used_since_last_human_text = True
                continue

            if _is_human_text_turn(msg):
                if tool_used_since_last_human_text:
                    # Tools were used between text turns — this is a new
                    # topic/instruction, not a reprompt. Reset the chain.
                    consecutive_human_text_count = 1
                    tool_used_since_last_human_text = False
                elif consecutive_human_text_count == 0:
                    # First human text turn in the session
                    consecutive_human_text_count = 1
                else:
                    # No tools used since last human text — this is a
                    # reprompt (user rephrasing after text-only response)
                    consecutive_human_text_count += 1
                    rephrase_count += 1
                    tool_used_since_last_human_text = False

    bail_out = rephrase_count >= threshold
    return rephrase_count, bail_out
