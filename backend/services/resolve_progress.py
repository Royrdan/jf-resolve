"""In-memory progress board for in-flight stream resolves.

A cold resolve takes 15-60s (candidate walk + ffprobe validation), during which
the caller — Jellyfin's ffprobe or Wholphin's own resolver GET — sits on a
blocking HTTP request with no idea what is happening. The player therefore shows
a spinner with invented, timer-driven text ("Still searching…") that says the
same thing whether the resolver is working or wedged.

This module lets `stream.py` narrate itself. Each phase publishes a ready-to-
display line against the resolve's cache key; `/api/stream/progress/...` reads it
back. Keying on WHAT is being resolved (not on who asked) is deliberate: the
first caller to arrive pays the cold cost, and which one that is varies per play
— Jellyfin probes the `.strm` when it has no cached media info, otherwise the
player's own request is first in. Both land on the same key.

Rules this module lives by:

* **It can never break a resolve.** Every public function swallows its own
  errors. Instrumentation that can fail playback is worse than no
  instrumentation.
* **It never grows.** Entries are pruned by age and the board is hard-capped.
* **Wording lives here, not in the app.** Changing a phrase must not require an
  APK rebuild, so the server ships a display-ready `message`.
"""

import time
from typing import Dict, Optional

# Completed entries linger briefly so a poll landing just after the resolve
# finishes still sees "done" rather than "idle" (which the app would read as
# "server doesn't support this").
_DONE_TTL_SECONDS = 90

# An entry that never reached a terminal state (process killed mid-walk, an
# exception path that skipped finish()) must not live forever.
_ACTIVE_TTL_SECONDS = 600

# Hard ceiling on board size. One household plays one thing at a time; this is a
# runaway guard, not a working limit.
_MAX_ENTRIES = 50

_BOARD: Dict[str, dict] = {}


def _now() -> float:
    return time.time()


def _expired(entry: dict, now: float) -> bool:
    age = now - entry.get("updated", 0)
    if entry.get("state") == "active":
        return age > _ACTIVE_TTL_SECONDS
    return age > _DONE_TTL_SECONDS


def _prune(now: float) -> None:
    for key in [k for k, e in _BOARD.items() if _expired(e, now)]:
        _BOARD.pop(key, None)
    # Still oversized (many distinct keys inside one TTL window): drop the
    # least-recently-updated entries first.
    if len(_BOARD) > _MAX_ENTRIES:
        for key, _ in sorted(
            _BOARD.items(), key=lambda kv: kv[1].get("updated", 0)
        )[: len(_BOARD) - _MAX_ENTRIES]:
            _BOARD.pop(key, None)


def begin(key: str, message: str, **detail) -> None:
    """Open an entry for `key` — a no-op if one is already active.

    Resolves coalesce: a second request for the same key waits on the leader's
    lock. Without this guard the waiter's opening publish would overwrite the
    leader's live phase, making the on-screen status jump backwards.
    """
    try:
        now = _now()
        existing = _BOARD.get(key)
        if (
            existing is not None
            and existing.get("state") == "active"
            and not _expired(existing, now)
        ):
            existing["updated"] = now
            _prune(now)
            return
        _BOARD[key] = {
            "key": key,
            "state": "active",
            "phase": "start",
            "message": message,
            "detail": dict(detail),
            "started": now,
            "updated": now,
        }
        # Pruned AFTER the write, so the cap counts this entry. Pruning first
        # let the board settle one over the limit on every insert.
        _prune(now)
    except Exception:
        pass


def publish(key: str, phase: str, message: str, **detail) -> None:
    """Record the current phase of an in-flight resolve."""
    try:
        now = _now()
        entry = _BOARD.get(key)
        if entry is None:
            entry = {"key": key, "started": now}
            _BOARD[key] = entry
        entry["state"] = "active"
        entry["phase"] = phase
        entry["message"] = message
        entry["detail"] = dict(detail)
        entry["updated"] = now
        _prune(now)
    except Exception:
        pass


def finish(key: str, ok: bool, message: str, **detail) -> None:
    """Close an entry out as done or failed."""
    try:
        now = _now()
        entry = _BOARD.get(key)
        if entry is None:
            entry = {"key": key, "started": now, "phase": "done" if ok else "failed"}
            _BOARD[key] = entry
        entry["state"] = "done" if ok else "failed"
        entry["message"] = message
        if detail:
            entry["detail"] = dict(detail)
        entry.setdefault("detail", {})
        entry["updated"] = now
        _prune(now)
    except Exception:
        pass


def get(key: str) -> Optional[dict]:
    """Display-ready snapshot for `key`, or None when nothing is known.

    `elapsed_ms` is how long this resolve has been running; `updated_ms_ago`
    lets the caller spot a stalled phase (a number that keeps climbing while the
    message stays put is a phase that is taking too long).
    """
    try:
        now = _now()
        entry = _BOARD.get(key)
        if entry is None:
            return None
        if _expired(entry, now):
            _BOARD.pop(key, None)
            return None
        return {
            "state": entry.get("state", "active"),
            "phase": entry.get("phase", ""),
            "message": entry.get("message", ""),
            "detail": entry.get("detail", {}),
            "elapsed_ms": int((now - entry.get("started", now)) * 1000),
            "updated_ms_ago": int((now - entry.get("updated", now)) * 1000),
        }
    except Exception:
        return None


def reset() -> None:
    """Clear the board (tests only)."""
    _BOARD.clear()
