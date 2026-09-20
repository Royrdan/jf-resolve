"""Tests for the resolve progress board (services/resolve_progress).

The board narrates an in-flight resolve to the player's loading page. Three
properties matter enough to lock down:

* a coalesced second request must not rewind the leader's phase (`begin` is a
  no-op on an active entry) — otherwise the on-screen status jumps backwards;
* nothing it does may ever raise, because a resolve calls it ~14 times and
  instrumentation that can fail playback is worse than none;
* it must not grow without bound in a long-lived process.

Loaded straight from the file: the module is stdlib-only by design, and
importing through `backend.services` would drag in the auth stack.
"""
import importlib.util
import time
from pathlib import Path

import pytest

MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "services" / "resolve_progress.py"
)


def _load():
    spec = importlib.util.spec_from_file_location("resolve_progress", MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def rp():
    mod = _load()
    mod.reset()
    return mod


def test_unknown_key_is_none(rp):
    assert rp.get("tv:1:1:1:auto:0") is None


def test_publish_then_get_round_trips(rp):
    rp.begin("k", "Getting ready…")
    rp.publish("k", "walk", "Checking source 2 of 9", index=2, total=9)

    snap = rp.get("k")
    assert snap["state"] == "active"
    assert snap["phase"] == "walk"
    assert snap["message"] == "Checking source 2 of 9"
    assert snap["detail"] == {"index": 2, "total": 9}
    assert snap["elapsed_ms"] >= 0
    assert snap["updated_ms_ago"] >= 0


def test_begin_does_not_rewind_an_active_entry(rp):
    """The coalescing case: leader is mid-walk, a second caller arrives."""
    rp.begin("k", "Getting ready…")
    rp.publish("k", "walk", "Checking source 3 of 9")

    rp.begin("k", "Getting ready…")  # second request for the same key

    snap = rp.get("k")
    assert snap["phase"] == "walk"
    assert snap["message"] == "Checking source 3 of 9"


def test_begin_reopens_after_a_finished_resolve(rp):
    rp.begin("k", "Getting ready…")
    rp.finish("k", True, "Source found — starting playback")
    assert rp.get("k")["state"] == "done"

    rp.begin("k", "Getting ready…")
    snap = rp.get("k")
    assert snap["state"] == "active"
    assert snap["phase"] == "start"


def test_finish_records_failure_reason(rp):
    rp.begin("k", "Getting ready…")
    rp.finish("k", False, "Only cam-tier or foreign-dub sources available (blocked)")

    snap = rp.get("k")
    assert snap["state"] == "failed"
    assert "cam-tier" in snap["message"]


def test_finish_without_begin_still_records(rp):
    """A cache hit finishes a resolve that never opened an entry."""
    rp.finish("k", True, "Ready — starting playback", cached=True)
    assert rp.get("k")["state"] == "done"


def test_completed_entries_expire(rp, monkeypatch):
    rp.begin("k", "Getting ready…")
    rp.finish("k", True, "Source found — starting playback")

    later = time.time() + rp._DONE_TTL_SECONDS + 1
    monkeypatch.setattr(rp, "_now", lambda: later)
    assert rp.get("k") is None


def test_abandoned_active_entries_expire(rp, monkeypatch):
    """A resolve killed mid-walk never calls finish; it must not live forever."""
    rp.begin("k", "Getting ready…")

    later = time.time() + rp._ACTIVE_TTL_SECONDS + 1
    monkeypatch.setattr(rp, "_now", lambda: later)
    assert rp.get("k") is None


def test_active_entries_outlive_the_done_ttl(rp, monkeypatch):
    """A genuinely slow resolve must not vanish from under the loading page."""
    rp.begin("k", "Getting ready…")

    later = time.time() + rp._DONE_TTL_SECONDS + 1
    monkeypatch.setattr(rp, "_now", lambda: later)
    assert rp.get("k")["state"] == "active"


def test_board_is_capped(rp):
    for i in range(rp._MAX_ENTRIES * 2):
        rp.begin(f"k{i}", "Getting ready…")
    assert len(rp._BOARD) <= rp._MAX_ENTRIES
    # The most recent writes survive; the oldest are dropped.
    assert rp.get(f"k{rp._MAX_ENTRIES * 2 - 1}") is not None


def test_publish_never_raises(rp):
    """Instrumentation must not be able to fail a resolve."""

    class Exploding:
        def __repr__(self):
            raise RuntimeError("boom")

        def __eq__(self, other):
            raise RuntimeError("boom")

        __hash__ = None  # unhashable: breaks anything that tries to key on it

    rp.begin("k", "Getting ready…")
    rp.publish("k", "walk", "Checking source 1 of 9", hostile=Exploding())
    rp.finish("k", True, "Source found — starting playback")
    assert rp.get("k")["state"] == "done"


def test_get_discards_a_corrupt_entry_instead_of_raising(rp):
    rp._BOARD["k"] = {"state": "active"}  # missing started/updated
    assert rp.get("k") is None  # treated as stale and dropped
    assert "k" not in rp._BOARD
