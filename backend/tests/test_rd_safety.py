#!/usr/bin/env python3
"""
Mock-Real-Debrid safety tests for jf-resolve.

Runs the REAL RDService code against a fully in-process fake Real-Debrid
(httpx.MockTransport) — no network, no account, no risk. Proves the guardrails
added after the 2026-08-01 single-IP storm actually hold:

  1. A cached torrent resolves to a CDN link (happy path still works).
  2. An uncached torrent is cleaned up (deleted) and returns None.
  3. A filter-gate 451 is handled without blowing up.
  4. The rolling-window HARD CAP bounds total RD calls no matter how hard the
     caller loops (the structural anti-storm guarantee).
  5. A 429 trips the circuit breaker and every later call short-circuits
     WITHOUT hitting RD (graceful failure, no brute-forcing).
  6. rd_filename_blocked() skips the release tags RD rejects.

Run:  python3 backend/tests/test_rd_safety.py       (from repo root)
   or: pytest backend/tests/test_rd_safety.py
"""

import asyncio
import importlib.util
import sys
import types
from pathlib import Path
from urllib.parse import parse_qs

import httpx

# Load ONLY the RD module chain (log_service → stream_validator → rd_service),
# bypassing backend/services/__init__.py which eagerly imports the whole app
# (FastAPI, python-jose, sqlite, …). We register stub `backend` /
# `backend.services` packages so rd_service's relative imports resolve without
# running that __init__. Keeps the test dependency-light: httpx only.
_SERVICES = Path(__file__).resolve().parents[1] / "services"


def _stub_pkg(name, path):
    pkg = types.ModuleType(name)
    pkg.__path__ = [str(path)]
    sys.modules[name] = pkg


def _load(modname, filepath):
    spec = importlib.util.spec_from_file_location(modname, filepath)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[modname] = mod
    spec.loader.exec_module(mod)
    return mod


_stub_pkg("backend", _SERVICES.parent)
_stub_pkg("backend.services", _SERVICES)
_load("backend.services.log_service", _SERVICES / "log_service.py")
_load("backend.services.stream_validator", _SERVICES / "stream_validator.py")
_rd = _load("backend.services.rd_service", _SERVICES / "rd_service.py")

RDService = _rd.RDService
rd_filename_blocked = _rd.rd_filename_blocked
RD_BLOCKED_RELEASE_TAGS = _rd.RD_BLOCKED_RELEASE_TAGS

BASE = RDService.BASE_URL


# ---------------------------------------------------------------------------
# Fake Real-Debrid
# ---------------------------------------------------------------------------
class MockRD:
    """
    In-process Real-Debrid. `behavior` maps an infohash prefix to one of:
    'cached' | 'uncached' | 'blocked' | 'storm'. Counts every HTTP call so
    tests can assert the rails bounded the volume.
    """

    def __init__(self, behavior=None, library=None):
        self.behavior = behavior or {}
        self.library = library or []          # GET /torrents entries
        self.calls = []                       # (method, path) of every hit
        self._torrents = {}                   # id -> infohash
        self._seq = 0

    def _kind(self, infohash: str) -> str:
        for pref, kind in self.behavior.items():
            if infohash.startswith(pref):
                return kind
        return "cached"

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        method = request.method
        self.calls.append((method, path))

        # GET /torrents  (library list)
        if method == "GET" and path.endswith("/torrents"):
            return httpx.Response(200, json=self.library)

        # POST /torrents/addMagnet
        if method == "POST" and path.endswith("/torrents/addMagnet"):
            body = parse_qs(request.content.decode())
            magnet = body.get("magnet", [""])[0]
            infohash = magnet.split("btih:")[-1]
            kind = self._kind(infohash)
            if kind == "storm":
                return httpx.Response(429, json={"error": "too_many_requests"})
            if kind == "blocked":
                return httpx.Response(
                    451, json={"error": "infringing_file", "error_code": 35}
                )
            self._seq += 1
            tid = f"t{self._seq}"
            self._torrents[tid] = infohash
            return httpx.Response(201, json={"id": tid, "uri": magnet})

        # GET /torrents/info/{id}
        if method == "GET" and "/torrents/info/" in path:
            tid = path.rsplit("/", 1)[-1]
            infohash = self._torrents.get(tid, "")
            kind = self._kind(infohash)
            if kind == "uncached":
                return httpx.Response(200, json={"status": "downloading",
                                                 "files": [], "links": []})
            # cached → instantly downloaded, one playable file + link
            return httpx.Response(200, json={
                "status": "downloaded",
                "files": [{"id": 1, "path": "/Movie.2020.1080p.mkv",
                           "bytes": 900, "selected": 1}],
                "links": ["https://real-debrid.com/d/HOSTERLINK"],
            })

        # POST /torrents/selectFiles/{id}
        if method == "POST" and "/torrents/selectFiles/" in path:
            return httpx.Response(204)

        # POST /unrestrict/link
        if method == "POST" and path.endswith("/unrestrict/link"):
            return httpx.Response(200, json={
                "download": "https://download.real-debrid.com/d/ABC/Movie.mkv"})

        # DELETE /torrents/delete/{id}
        if method == "DELETE" and "/torrents/delete/" in path:
            return httpx.Response(204)

        return httpx.Response(404, json={"error": "unknown_endpoint"})


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------
def _install(mock: MockRD):
    """Point RDService at the fake RD and clear ALL shared state."""
    RDService._transport = httpx.MockTransport(mock.handler)
    RDService._cache = {}
    RDService._info_cache = {}
    RDService._reset_rails()


def _teardown():
    RDService._transport = None
    RDService._reset_rails()


_results = []


def check(name, cond, detail=""):
    _results.append((name, bool(cond), detail))
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail and not cond else ""))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
async def test_cached_resolves():
    mock = MockRD(behavior={"aa": "cached"})
    _install(mock)
    rd = RDService("fake-key")
    url = await rd.resolve_infohash("aa" + "0" * 38)
    _teardown()
    check("cached torrent resolves to a CDN link",
          url == "https://download.real-debrid.com/d/ABC/Movie.mkv",
          f"got {url!r}")


async def test_uncached_cleans_up():
    mock = MockRD(behavior={"bb": "uncached"})
    _install(mock)
    rd = RDService("fake-key")
    url = await rd.resolve_infohash("bb" + "0" * 38)
    deleted = any(m == "DELETE" for m, _ in mock.calls)
    _teardown()
    check("uncached torrent returns None", url is None, f"got {url!r}")
    check("uncached torrent is deleted (no orphan on account)", deleted)


async def test_blocked_451():
    mock = MockRD(behavior={"cc": "blocked"})
    _install(mock)
    rd = RDService("fake-key")
    url = await rd.resolve_infohash("cc" + "0" * 38)
    _teardown()
    check("451 filter-gate handled gracefully (None, no crash)", url is None,
          f"got {url!r}")


async def test_hard_cap_bounds_a_storm():
    """Even if the CALLER loops forever, the rolling-window cap + breaker bound
    total RD HTTP calls. This is the structural anti-storm guarantee."""
    mock = MockRD(behavior={"dd": "uncached"})
    _install(mock)
    RDService.MIN_INTERVAL = 0.0            # don't wait real seconds in the test
    RDService.MAX_CALLS_PER_WINDOW = 20     # small cap so the test is quick
    RDService.WINDOW_SECONDS = 60.0
    rd = RDService("fake-key")
    # A pathological caller: 50 distinct uncached candidates back-to-back.
    for i in range(50):
        await rd.resolve_infohash(f"dd{i:038d}")
    total = len(mock.calls)
    breaker = RDService._breaker_open()
    # restore defaults
    RDService.MIN_INTERVAL = 0.6
    RDService.MAX_CALLS_PER_WINDOW = 45
    _teardown()
    check("hard cap bounds a 50-candidate storm",
          total <= 21, f"RD saw {total} calls (cap 20)")
    check("breaker is OPEN after hitting the cap", breaker)


async def test_429_trips_breaker():
    mock = MockRD(behavior={"ee": "storm"})
    _install(mock)
    RDService.MIN_INTERVAL = 0.0
    RDService.MAX_RETRIES = 0              # trip immediately, no backoff sleeps
    rd = RDService("fake-key")
    await rd.resolve_infohash("ee" + "0" * 38)   # first play → 429 → trip
    calls_after_first = len(mock.calls)
    breaker = RDService._breaker_open()
    # A second play while cooling down must NOT touch RD at all.
    await rd.resolve_infohash("ee" + "1" * 38)
    calls_after_second = len(mock.calls)
    RDService.MAX_RETRIES = 2
    RDService.MIN_INTERVAL = 0.6
    _teardown()
    check("429 trips the circuit breaker", breaker)
    check("calls during cooldown short-circuit (RD untouched)",
          calls_after_second == calls_after_first,
          f"{calls_after_first} → {calls_after_second}")


def test_filename_blocked():
    tags = RD_BLOCKED_RELEASE_TAGS
    check("YTS release is flagged blocked",
          rd_filename_blocked("The.Movie.2020.1080p.BluRay.x264-YTS.mx", tags))
    check("RARBG release is flagged blocked",
          rd_filename_blocked("Show.S01E01.1080p.RARBG.mkv", tags))
    check("clean WEB-DL release is NOT over-blocked",
          not rd_filename_blocked("The.Movie.2020.1080p.WEB-DL.DDP5.1.mkv", tags))
    check("word-boundary: 'ytstuff' does not false-match 'yts'",
          not rd_filename_blocked("ytstuff.and.things.mkv", tags))


# ---------------------------------------------------------------------------
async def main():
    print("Mock-RD safety tests (no network, no account touched):")
    await test_cached_resolves()
    await test_uncached_cleans_up()
    await test_blocked_451()
    await test_hard_cap_bounds_a_storm()
    await test_429_trips_breaker()
    test_filename_blocked()

    passed = sum(1 for _, ok, _ in _results if ok)
    total = len(_results)
    print(f"\n{passed}/{total} checks passed.")
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
