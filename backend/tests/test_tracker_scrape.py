"""Tests for the UDP tracker scrape (services/tracker_scrape).

The scrape feeds the uncached-load gate, which DROPS candidates. That makes two
properties safety-critical:

* it must never raise — a tracker outage, blocked UDP or a malformed datagram
  has to degrade to "unknown", because the caller reads unknown as "allow" and
  anything else would turn a dead tracker into an unplayable library;
* a hash absent from the result must never be confused with a 0-seed hash. Zero
  means "provably dead, drop it"; absent means "we have no idea, keep it".

The byte-level response parsing is covered against a crafted datagram — it is
the one part that cannot be checked by reading it.

Loaded straight from the file with a stubbed log_service, so the test does not
drag in the auth/database stack.
"""
import asyncio
import importlib.util
import struct
import sys
import types
from pathlib import Path

import pytest

MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "services" / "tracker_scrape.py"
)


def _load():
    for name in ("backend", "backend.services"):
        if name not in sys.modules:
            pkg = types.ModuleType(name)
            pkg.__path__ = []
            sys.modules[name] = pkg

    class _Log:
        def info(self, *a, **k):
            pass

        def warning(self, *a, **k):
            pass

    stub = types.ModuleType("backend.services.log_service")
    stub.log_service = _Log()
    sys.modules["backend.services.log_service"] = stub

    spec = importlib.util.spec_from_file_location(
        "backend.services.tracker_scrape", MODULE_PATH
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


ts = _load()

HASH_A = "02e64c0e32782cf7c3130465e10b71ea7312491c"
HASH_B = "d3b5813d60029c22e4cb9d862f1861a5ce7b7332"


# ── URL parsing ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "entry,expected",
    [
        ("udp://tracker.opentrackr.org:1337", ("tracker.opentrackr.org", 1337)),
        ("udp://open.stealth.si:80/announce", ("open.stealth.si", 80)),
        ("open.stealth.si:80", ("open.stealth.si", 80)),
        ("https://tracker.example.org:443", None),  # HTTP scrape is a different wire format
        ("udp://no-port-here", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_tracker(entry, expected):
    assert ts._parse_tracker(entry) == expected


# ── hash normalisation ─────────────────────────────────────────────────────


def test_normalise_hashes_dedupes_lowercases_and_validates():
    out = ts._normalise_hashes(
        [HASH_A.upper(), HASH_A, HASH_B, "tooshort", "z" * 40, "", None]
    )
    assert out == [HASH_A, HASH_B]


def test_normalise_hashes_preserves_order():
    assert ts._normalise_hashes([HASH_B, HASH_A]) == [HASH_B, HASH_A]


# ── byte-level protocol parsing ────────────────────────────────────────────


class _FakeSocket:
    """Minimal UDP socket returning canned connect + scrape datagrams."""

    def __init__(self, seeder_table, *, error_message=None, truncate_to=None):
        self.seeder_table = seeder_table
        self.error_message = error_message
        self.truncate_to = truncate_to
        self.sent = []
        self._pending = None
        self._connection_id = 0x1122334455667788

    def settimeout(self, _t):
        pass

    def close(self):
        pass

    def sendto(self, payload, _addr):
        self.sent.append(payload)
        action, txn = struct.unpack(">II", payload[8:16])
        if action == ts._ACTION_CONNECT:
            self._pending = struct.pack(
                ">IIQ", ts._ACTION_CONNECT, txn, self._connection_id
            )
            return
        if self.error_message is not None:
            self._pending = (
                struct.pack(">II", ts._ACTION_ERROR, txn) + self.error_message
            )
            return
        body = b""
        for i in range(16, len(payload), 20):
            infohash = payload[i : i + 20].hex()
            seeders = self.seeder_table.get(infohash, 0)
            body += struct.pack(">III", seeders, 0, 0)
        data = struct.pack(">II", ts._ACTION_SCRAPE, txn) + body
        self._pending = data[: self.truncate_to] if self.truncate_to else data

    def recvfrom(self, _bufsize):
        return self._pending, ("127.0.0.1", 1337)


def _patch_socket(monkeypatch, fake):
    monkeypatch.setattr(ts.socket, "gethostbyname", lambda h: "127.0.0.1")
    monkeypatch.setattr(ts.socket, "socket", lambda *a, **k: fake)


def test_scrape_parses_seeder_counts(monkeypatch):
    fake = _FakeSocket({HASH_A: 0, HASH_B: 26})
    _patch_socket(monkeypatch, fake)
    out = ts._scrape_one_blocking("t", 1337, [HASH_A, HASH_B], 1.0)
    assert out == {HASH_A: 0, HASH_B: 26}


def test_scrape_batches_above_packet_ceiling(monkeypatch):
    hashes = [f"{i:040x}" for i in range(ts._MAX_HASHES_PER_PACKET + 10)]
    fake = _FakeSocket({h: 1 for h in hashes})
    _patch_socket(monkeypatch, fake)
    out = ts._scrape_one_blocking("t", 1337, hashes, 1.0)
    assert len(out) == len(hashes)
    # 1 connect + 2 scrape datagrams
    assert len(fake.sent) == 3


def test_scrape_raises_on_tracker_error(monkeypatch):
    _patch_socket(monkeypatch, _FakeSocket({}, error_message=b"rate limited"))
    with pytest.raises(ValueError):
        ts._scrape_one_blocking("t", 1337, [HASH_A], 1.0)


def test_scrape_tolerates_truncated_response(monkeypatch):
    """A tracker returning fewer entries than asked must yield what arrived,
    not raise and not invent zeros for the missing tail."""
    fake = _FakeSocket({HASH_A: 5, HASH_B: 9}, truncate_to=8 + 12)
    _patch_socket(monkeypatch, fake)
    out = ts._scrape_one_blocking("t", 1337, [HASH_A, HASH_B], 1.0)
    assert out == {HASH_A: 5}


# ── aggregation + failure behaviour ────────────────────────────────────────


@pytest.mark.asyncio
async def test_empty_input_short_circuits(monkeypatch):
    def _boom(*a, **k):
        raise AssertionError("must not touch the network for an empty list")

    monkeypatch.setattr(ts, "_scrape_one_blocking", _boom)
    assert await ts.scrape_seeders([]) == {}


@pytest.mark.asyncio
async def test_highest_count_across_trackers_wins(monkeypatch):
    def fake(host, port, hashes, timeout):
        return {HASH_A: 3} if host == "a" else {HASH_A: 11}

    monkeypatch.setattr(ts, "_scrape_one_blocking", fake)
    out = await ts.scrape_seeders([HASH_A], ["udp://a:1", "udp://b:2"])
    assert out == {HASH_A: 11}


@pytest.mark.asyncio
async def test_zero_is_preserved_not_dropped(monkeypatch):
    """A provable 0 must survive aggregation — it is what lets the caller drop
    a dead magnet. Merging must not confuse it with 'no answer'."""
    monkeypatch.setattr(
        ts, "_scrape_one_blocking", lambda *a, **k: {HASH_A: 0}
    )
    assert await ts.scrape_seeders([HASH_A], ["udp://a:1"]) == {HASH_A: 0}


@pytest.mark.asyncio
async def test_all_trackers_failing_yields_unknown_not_zero(monkeypatch):
    def boom(*a, **k):
        raise OSError("network unreachable")

    monkeypatch.setattr(ts, "_scrape_one_blocking", boom)
    out = await ts.scrape_seeders([HASH_A, HASH_B], ["udp://a:1", "udp://b:2"])
    assert out == {}  # absent == unknown == allow; NOT {hash: 0}


@pytest.mark.asyncio
async def test_partial_tracker_failure_still_returns_data(monkeypatch):
    def fake(host, port, hashes, timeout):
        if host == "a":
            raise OSError("down")
        return {HASH_B: 7}

    monkeypatch.setattr(ts, "_scrape_one_blocking", fake)
    assert await ts.scrape_seeders([HASH_B], ["udp://a:1", "udp://b:2"]) == {HASH_B: 7}


@pytest.mark.asyncio
async def test_unexpected_exception_is_contained(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("something nobody predicted")

    monkeypatch.setattr(ts, "_scrape_one_blocking", boom)
    assert await ts.scrape_seeders([HASH_A], ["udp://a:1"]) == {}


@pytest.mark.asyncio
async def test_hung_tracker_does_not_block_forever(monkeypatch):
    def hang(*a, **k):
        import time as _t

        _t.sleep(5)
        return {HASH_A: 1}

    monkeypatch.setattr(ts, "_scrape_one_blocking", hang)
    out = await asyncio.wait_for(
        ts.scrape_seeders([HASH_A], ["udp://a:1"], timeout=0.2), timeout=4
    )
    assert out == {}


@pytest.mark.asyncio
async def test_unusable_tracker_list_skips_cleanly(monkeypatch):
    def _boom(*a, **k):
        raise AssertionError("must not dial an unparseable tracker")

    monkeypatch.setattr(ts, "_scrape_one_blocking", _boom)
    assert await ts.scrape_seeders([HASH_A], ["https://nope:443", "garbage"]) == {}
