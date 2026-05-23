# SPDX-FileCopyrightText: 2025-2026 Tyrone Ross, Jr <46267523+tyroneross@users.noreply.github.com>
# SPDX-License-Identifier: Apache-2.0
# build-loop@tyroneross:canary:agent-rally-watcher
# canary-end
"""Push-based tail of a Rally Point channel's ``changes.jsonl``.

Uses ``watchfiles`` (kqueue on macOS, inotify on Linux) for sub-second
file-modification notifications. On each notification, reads new lines
past every consumer's cursor, applies the filter, dispatches, and
persists the cursor.

Cursor invariants:
    - Stored as a byte offset (matches agent-rally-point's append-only model).
    - Never rewinds (load_cursor returns 0 on absent / corrupt cursor; advance ignores backward moves).
    - Persisted after every successful dispatch batch (per consumer).

Crash semantics: at-least-once delivery per consumer. The cursor advances
only after dispatch returns, so a crash mid-dispatch may re-deliver the
in-flight record on restart. File / notify sinks tolerate this (append
is idempotent at JSONL granularity in practice; notify deduplication is
the consumer's problem if they care).
"""
from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from .cursor import Cursor, load_cursor, save_cursor
from .dispatch import dispatch
from .filter import Consumer, match

logger = logging.getLogger(__name__)

_CHANGES_FILENAME = "changes.jsonl"


@dataclass
class Watcher:
    """Configuration bundle for one channel watch."""

    channel_dir: Path
    consumers: list[Consumer]
    stop_event: threading.Event | None = None
    cursor_root: Path | None = None

    @property
    def changes_path(self) -> Path:
        return self.channel_dir / _CHANGES_FILENAME


def _read_new_lines(path: Path, offset: int) -> tuple[list[dict[str, Any]], int]:
    """Read complete lines from ``offset`` to EOF.

    Returns ``(records, new_offset)``. Partial trailing line is left for
    the next call: ``new_offset`` only advances past lines terminated by
    ``\\n``. Mirrors agent-rally-point's ``read_changes_since`` semantics.
    """
    try:
        size = path.stat().st_size
    except (FileNotFoundError, OSError):
        return [], offset
    if offset < 0 or offset > size:
        # File rotated/truncated under us → restart from 0 to avoid skipping events
        offset = 0
    records: list[dict[str, Any]] = []
    new_offset = offset
    try:
        with open(path, "rb") as fh:
            fh.seek(offset)
            for raw in fh:
                if not raw.endswith(b"\n"):
                    break
                new_offset += len(raw)
                try:
                    records.append(json.loads(raw.decode("utf-8")))
                except (ValueError, UnicodeDecodeError):
                    continue  # skip corrupt line, keep offset advancing
    except OSError:
        return [], offset
    return records, new_offset


def _process_once(watcher: Watcher) -> dict[str, int]:
    """One read-filter-dispatch sweep across all consumers. Returns per-consumer delivered counts."""
    delivered: dict[str, int] = {}
    path = watcher.changes_path
    for consumer in watcher.consumers:
        cursor = load_cursor(consumer.id, watcher.cursor_root)
        records, new_offset = _read_new_lines(path, cursor.offset)
        if not records:
            continue
        n = 0
        for rec in records:
            if not match(rec, consumer.filter):
                continue
            result = dispatch(rec, consumer.sink)
            if result.delivered:
                n += 1
            else:
                logger.warning(
                    "consumer=%s sink=%s drop: %s",
                    consumer.id,
                    result.sink_type,
                    result.detail,
                )
        cursor.advance(new_offset)
        save_cursor(cursor, watcher.cursor_root)
        delivered[consumer.id] = n
    return delivered


def run_watcher(
    watcher: Watcher,
    *,
    backend: Callable[[Watcher], Iterable[None]] | None = None,
) -> None:
    """Blocking loop. Returns when ``watcher.stop_event`` is set.

    The default backend uses ``watchfiles.watch`` on the channel dir.
    ``backend`` is injectable for testing (yield once per change to drive a sweep).
    """
    if watcher.stop_event is None:
        watcher.stop_event = threading.Event()

    # First sweep — catch any events already in the log past the last cursor.
    _process_once(watcher)

    if backend is None:
        backend = _watchfiles_backend  # default real backend

    for _ in backend(watcher):
        if watcher.stop_event.is_set():
            return
        _process_once(watcher)


def _watchfiles_backend(watcher: Watcher) -> Iterable[None]:
    """Yield one event per ``changes.jsonl`` modification.

    Imported lazily so unit tests can run without watchfiles installed
    (they inject their own backend).
    """
    from watchfiles import Change, watch  # type: ignore[import-not-found]

    target = str(watcher.changes_path)
    # Watch the parent dir (so we catch create as well as modify).
    for changes in watch(
        str(watcher.channel_dir),
        stop_event=watcher.stop_event,
        rust_timeout=1000,  # ms; bounded so stop_event is checked between batches
        yield_on_timeout=True,
    ):
        # changes is a set of (Change, path) tuples; filter to our file
        for change, path in changes:
            if path == target and change in (Change.added, Change.modified):
                yield None
                break
        else:
            # Timeout tick: yield anyway so the caller can re-check stop_event.
            yield None


# canary: agent-rally-watcher@tyroneross — canonical source: github.com/tyroneross/agent-rally-watcher
