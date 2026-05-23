# SPDX-FileCopyrightText: 2025-2026 Tyrone Ross, Jr <46267523+tyroneross@users.noreply.github.com>
# SPDX-License-Identifier: Apache-2.0
"""Watcher end-to-end: tail + filter + dispatch + cursor persistence.

Uses an injected synchronous backend (no watchfiles dependency in the test
process), so test runs in milliseconds and is hermetic.
"""
from __future__ import annotations

import json
import shutil
import threading
from pathlib import Path
from typing import Iterable

import pytest

from agent_rally_watcher.cursor import load_cursor
from agent_rally_watcher.filter import Consumer, FilterRule
from agent_rally_watcher.watcher import Watcher, _process_once, run_watcher

FIXTURE = Path(__file__).parent / "fixtures" / "sample_changes.jsonl"


def _seed_channel(channel_dir: Path) -> Path:
    """Copy the fixture into channel_dir/changes.jsonl."""
    channel_dir.mkdir(parents=True, exist_ok=True)
    target = channel_dir / "changes.jsonl"
    shutil.copy(FIXTURE, target)
    return target


def _consumer(tmp_path: Path, cid: str, rule: FilterRule) -> Consumer:
    return Consumer(
        id=cid,
        filter=rule,
        sink={"type": "file", "path": str(tmp_path / f"{cid}.out.jsonl")},
    )


def test_process_once_delivers_matched_records(tmp_path: Path) -> None:
    channel_dir = tmp_path / "channel"
    cursor_root = tmp_path / "cursors"
    _seed_channel(channel_dir)
    c_feedback = _consumer(tmp_path, "fb_only", FilterRule(kinds=["feedback"]))
    watcher = Watcher(channel_dir=channel_dir, consumers=[c_feedback], cursor_root=cursor_root)

    delivered = _process_once(watcher)
    assert delivered["fb_only"] == 2  # two feedback records in fixture

    out = (tmp_path / "fb_only.out.jsonl").read_text(encoding="utf-8").strip().split("\n")
    kinds = [json.loads(line)["kind"] for line in out]
    assert kinds == ["feedback", "feedback"]


def test_cursor_advances_and_resumes(tmp_path: Path) -> None:
    channel_dir = tmp_path / "channel"
    cursor_root = tmp_path / "cursors"
    changes = _seed_channel(channel_dir)
    rule = FilterRule()  # match everything
    consumer = _consumer(tmp_path, "all", rule)
    watcher = Watcher(channel_dir=channel_dir, consumers=[consumer], cursor_root=cursor_root)

    # First sweep — consume all 5 records
    _process_once(watcher)
    cursor1 = load_cursor("all", root=cursor_root)
    assert cursor1.offset == changes.stat().st_size

    # Append a 6th record
    with open(changes, "a", encoding="utf-8") as fh:
        fh.write(
            json.dumps(
                {
                    "ts": 1716480005.0,
                    "kind": "phase",
                    "tool": "claude_code",
                    "model": "claude-opus-4-7",
                    "run_id": "run-004",
                    "app_slug": "demo",
                    "payload": {"phase": "review"},
                    "revision": 6,
                }
            )
            + "\n"
        )

    # Second sweep — only the new record
    _process_once(watcher)
    out = (tmp_path / "all.out.jsonl").read_text(encoding="utf-8").strip().split("\n")
    assert len(out) == 6  # 5 from first sweep + 1 from second

    cursor2 = load_cursor("all", root=cursor_root)
    assert cursor2.offset == changes.stat().st_size
    assert cursor2.offset > cursor1.offset


def test_no_duplicate_on_restart(tmp_path: Path) -> None:
    """Simulate restart: rebuild watcher from cursor, expect zero re-delivery."""
    channel_dir = tmp_path / "channel"
    cursor_root = tmp_path / "cursors"
    _seed_channel(channel_dir)
    rule = FilterRule(kinds=["feedback"])
    c1 = _consumer(tmp_path, "fb", rule)
    w1 = Watcher(channel_dir=channel_dir, consumers=[c1], cursor_root=cursor_root)
    _process_once(w1)
    initial_lines = (tmp_path / "fb.out.jsonl").read_text(encoding="utf-8").count("\n")

    # "Restart" — fresh Watcher, same cursor_root
    c2 = _consumer(tmp_path, "fb", rule)
    w2 = Watcher(channel_dir=channel_dir, consumers=[c2], cursor_root=cursor_root)
    delivered = _process_once(w2)
    assert delivered.get("fb", 0) == 0

    final_lines = (tmp_path / "fb.out.jsonl").read_text(encoding="utf-8").count("\n")
    assert final_lines == initial_lines


def test_partial_trailing_line_held_for_next_sweep(tmp_path: Path) -> None:
    """A line without a trailing newline must not advance the cursor past it."""
    channel_dir = tmp_path / "channel"
    cursor_root = tmp_path / "cursors"
    changes = _seed_channel(channel_dir)
    # Append a partial line (no trailing newline)
    partial = json.dumps({"ts": 1.0, "kind": "feedback", "tool": "codex", "model": "x", "run_id": "r9", "app_slug": "demo", "payload": {"verdict": "PASS"}, "revision": 99})
    with open(changes, "a", encoding="utf-8") as fh:
        fh.write(partial)

    rule = FilterRule()
    consumer = _consumer(tmp_path, "all", rule)
    watcher = Watcher(channel_dir=channel_dir, consumers=[consumer], cursor_root=cursor_root)
    _process_once(watcher)

    # Cursor sits at the byte before the partial line
    cursor = load_cursor("all", root=cursor_root)
    expected = changes.stat().st_size - len(partial.encode("utf-8"))
    assert cursor.offset == expected

    # Complete the line — next sweep picks it up
    with open(changes, "a", encoding="utf-8") as fh:
        fh.write("\n")
    _process_once(watcher)
    cursor2 = load_cursor("all", root=cursor_root)
    assert cursor2.offset == changes.stat().st_size


def test_run_watcher_drives_via_injected_backend(tmp_path: Path) -> None:
    """``run_watcher`` invokes _process_once for each backend yield."""
    channel_dir = tmp_path / "channel"
    cursor_root = tmp_path / "cursors"
    _seed_channel(channel_dir)
    rule = FilterRule(kinds=["feedback"])
    consumer = _consumer(tmp_path, "fb", rule)
    stop = threading.Event()
    watcher = Watcher(
        channel_dir=channel_dir,
        consumers=[consumer],
        stop_event=stop,
        cursor_root=cursor_root,
    )

    def backend(w: Watcher) -> Iterable[None]:
        yield None  # one tick, then stop
        stop.set()
        yield None

    run_watcher(watcher, backend=backend)

    # All 2 feedback records delivered (initial sweep + one backend tick = still 2 total)
    lines = (tmp_path / "fb.out.jsonl").read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
