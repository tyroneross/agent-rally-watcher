# Agent Rally Watcher

> **Daemon companion to [agent-rally-point](https://github.com/tyroneross/agent-rally-point): push-based watcher for Rally Point streams, with per-consumer filtering and dispatch.**

Rally Point is the substrate (event posting, presence, channels). Rally Watcher is the long-running listener that tails the channel's `changes.jsonl` and pushes filtered events to the tools that need them (Claude Code sessions, Codex CLI, terminals, hosted endpoints). Same architectural pattern as build-loop ↔ build-loop-monitor — substrate vs daemon, separated by concern.

## Status

**v0.1.0 — alpha** (2026-05-23). Push-based via [`watchfiles`](https://watchfiles.helpmanual.io/) (kqueue on macOS, inotify on Linux). Per-consumer cursor persistence. stdout + macOS notification dispatch. HTTP POST dispatch stubbed for v0.2.

## What it does

- **Push-based tail** of `~/.agent-rally-point/apps/<slug>/changes.jsonl` (sub-second on kqueue/inotify; no polling).
- **Per-consumer filtering**: `consumers.yaml` maps tool-id (`claude_code`, `codex`, ...) to filter rules (kinds, senders, payload-field matches).
- **Cursor persistence**: each consumer has a cursor at `~/.agent-rally-watcher/consumers/<tool>.cursor` so a daemon restart resumes mid-stream without dupes or gaps.
- **Dispatch sinks (v0.1)**: append-to-file (stdout-pipe target), macOS `osascript` notify. HTTP POST stubbed.
- **Daemon lifecycle**: `agent-rally-watcher start | stop | status | reload`. PID file + size-bounded log rotation at `~/.agent-rally-watcher/logs/daemon.log`.
- **macOS launchd**: plist generator (`agent-rally-watcher install-launchd`) for boot-time autostart. Linux/systemd planned.

## Install

```bash
uv pip install agent-rally-watcher   # or: pip install agent-rally-watcher
```

PyPI publication pending. Install from source until then:

```bash
git clone https://github.com/tyroneross/agent-rally-watcher.git
cd agent-rally-watcher
uv pip install -e .
```

## Quickstart

```bash
# 1. Author a consumers config
cp examples/consumers.yaml ~/.agent-rally-watcher/consumers.yaml
$EDITOR ~/.agent-rally-watcher/consumers.yaml

# 2. Start the daemon (current repo's channel — derived from `git rev-parse --git-common-dir`)
agent-rally-watcher start

# 3. Check status / tail logs
agent-rally-watcher status
tail -F ~/.agent-rally-watcher/logs/daemon.log

# 4. Stop the daemon
agent-rally-watcher stop
```

The daemon discovers the current repo's Rally Point channel via the same `app_slug()` logic agent-rally-point uses — worktree-independent, same channel across all clones of the canonical repo.

## consumers.yaml

```yaml
# Each entry: filter + sink. Filter rules are AND-combined; missing fields
# match everything. Sinks: file (append JSONL), notify (macOS osascript).
consumers:
  claude_code:
    filter:
      kinds: [feedback, handoff, dep-change]
      tools_not: [claude_code]    # don't echo back to self
    sink:
      type: file
      path: ~/.agent-rally-watcher/streams/claude_code.jsonl

  codex:
    filter:
      kinds: [feedback, handoff]
    sink:
      type: file
      path: ~/.agent-rally-watcher/streams/codex.jsonl

  urgent_notify:
    filter:
      kinds: [feedback]
      payload_match:
        verdict: BLOCKED
    sink:
      type: notify
      title: "Rally Watcher"
```

## Architecture

```
agent-rally-point          (substrate)
   writes ──► ~/.agent-rally-point/apps/<slug>/changes.jsonl
                         │
                         │  watchfiles (kqueue / inotify push)
                         ▼
agent-rally-watcher       (daemon)
   filter.match(record, consumer.rules)
   cursor.advance(consumer_id, byte_offset)
   dispatch.send(record, consumer.sink)
                         │
                         ▼
   ┌─────────────┬───────┴────────┬──────────────┐
   ▼             ▼                ▼              ▼
 file sink   notify sink      http POST       (custom)
 (stream)    (osascript)      (v0.2 stub)
```

## How this differs from `tail -F | grep`

- **Per-consumer cursors** — restart-safe, no duplicate delivery, no missed events.
- **Schema-aware filters** — match on `kind`, `tool`, or any `payload.<field>` without writing jq.
- **Structured dispatch** — notify, file, HTTP all from one config; no shell-pipeline plumbing.
- **Daemon lifecycle** — PID file, log rotation, launchd integration.

For one-shot tailing, `tail -F .../changes.jsonl | jq` is fine. For coordinating across multiple long-running tools, use the watcher.

## Provenance

Companion to [agent-rally-point](https://github.com/tyroneross/agent-rally-point); architectural sibling of [build-loop-monitor](https://github.com/tyroneross/build-loop). Tail engine: [`watchfiles`](https://github.com/samuelcolvin/watchfiles) (the only third-party dep).

## License & Attribution

Apache-2.0. See [LICENSE](./LICENSE).

- [`LICENSE`](LICENSE) — full license text.
- [`NOTICE`](NOTICE) — attribution notices that, per Apache 2.0 §4(d), must travel with any redistribution of this work.
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — contribution conventions: per-file SPDX headers (REUSE 3.3), AI co-author trailer, signed commits, conventional commits.

Per-file `SPDX-FileCopyrightText` and `SPDX-License-Identifier` headers are required on shipped source files. Files that cannot carry inline comments (JSON, generated assets) are annotated in [`REUSE.toml`](REUSE.toml). Validate compliance locally with `uvx reuse lint`.
