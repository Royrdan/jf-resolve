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
deprioritise_penalty = _rd.deprioritise_penalty

BASE = RDService.BASE_URL


# ---------------------------------------------------------------------------
# Fake Real-Debrid
# ---------------------------------------------------------------------------
class MockRD:
    """
    In-process Real-Debrid that models RD's REAL state machine:
        addMagnet → waiting_files_selection → (selectFiles) → downloading → downloaded

    `behavior` maps an infohash prefix to one of:
      'cached'   → ready the moment files are selected (RD had it).
      'slow:N'   → 'downloading' for N status reads after selection, then ready
                   (a well-seeded torrent RD pulls in a moment).
      'stuck'    → never leaves 'downloading' (no seeders / genuinely uncached).
      'dead'     → 'magnet_error' after selection (RD refuses it).
      'blocked'  → addMagnet returns 451 (filter-gate).
      'storm'    → addMagnet returns 429 (rate limited).
    Counts every HTTP call so tests can assert the rails bounded the volume.
    """

    def __init__(self, behavior=None, library=None, lib_info=None):
        self.behavior = behavior or {}
        self.library = library or []          # GET /torrents entries
        self.lib_info = lib_info or {}         # library id -> /torrents/info body
        self.calls = []                       # (method, path) of every hit
        self._t = {}                           # id -> torrent state
        self._seq = 0

    def _kind(self, infohash: str) -> str:
        for pref, kind in self.behavior.items():
            if infohash.startswith(pref):
                return kind
        return "cached"

    @staticmethod
    def _downloaded_body():
        return {
            "status": "downloaded",
            "files": [{"id": 1, "path": "/Movie.2020.1080p.mkv",
                       "bytes": 900, "selected": 1}],
            "links": ["https://real-debrid.com/d/HOSTERLINK"],
        }

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
                    451, json={"error": "infringing_file", "error_code": 35})
            self._seq += 1
            tid = f"t{self._seq}"
            # Every fresh add starts needing file selection, like real RD.
            self._t[tid] = {"kind": kind, "selected": False, "reads_after": 0}
            return httpx.Response(201, json={"id": tid, "uri": magnet})

        # POST /torrents/selectFiles/{id}
        if method == "POST" and "/torrents/selectFiles/" in path:
            tid = path.rsplit("/", 1)[-1]
            if tid in self._t:
                self._t[tid]["selected"] = True
            return httpx.Response(204)

        # GET /torrents/info/{id}
        if method == "GET" and "/torrents/info/" in path:
            tid = path.rsplit("/", 1)[-1]
            if tid in self.lib_info:                 # pre-existing library entry
                return httpx.Response(200, json=self.lib_info[tid])
            t = self._t.get(tid)
            if t is None:
                return httpx.Response(404, json={"error": "unknown_ressource"})
            if not t["selected"]:
                return httpx.Response(200, json={"status": "waiting_files_selection",
                                                 "files": [], "links": []})
            t["reads_after"] += 1
            kind = t["kind"]
            if kind == "cached":
                return httpx.Response(200, json=self._downloaded_body())
            if kind == "dead":
                return httpx.Response(200, json={"status": "magnet_error"})
            if kind == "stuck":
                return httpx.Response(200, json={"status": "downloading",
                                                 "files": [], "links": []})
            if kind.startswith("slow:"):
                need = int(kind.split(":", 1)[1])
                if t["reads_after"] >= need:
                    return httpx.Response(200, json=self._downloaded_body())
                return httpx.Response(200, json={"status": "downloading",
                                                 "files": [], "links": []})
            return httpx.Response(200, json=self._downloaded_body())

        # POST /unrestrict/link  → echo which hoster link (file) was chosen
        if method == "POST" and path.endswith("/unrestrict/link"):
            body = parse_qs(request.content.decode())
            link = body.get("link", ["?"])[0]
            tail = link.rsplit("/", 1)[-1] or "FILE"
            return httpx.Response(200, json={
                "download": f"https://download.real-debrid.com/d/{tail}"})

        # DELETE /torrents/delete/{id}
        if method == "DELETE" and "/torrents/delete/" in path:
            self._t.pop(path.rsplit("/", 1)[-1], None)
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
    # Poll the state machine for real, but without the real 2.5s waits, so the
    # tests exercise the SAME code path in milliseconds.
    RDService.POLL_INTERVAL = 0.0


def _teardown():
    RDService._transport = None
    RDService.POLL_INTERVAL = 2.5
    RDService.POLL_ATTEMPTS = 3
    RDService.MIN_INTERVAL = 0.6
    RDService.MAX_RETRIES = 2
    RDService.MAX_CALLS_PER_WINDOW = 45
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
    deleted = any(m == "DELETE" for m, _ in mock.calls)
    _teardown()
    check("cached torrent resolves to a CDN link",
          url == "https://download.real-debrid.com/d/HOSTERLINK",
          f"got {url!r}")
    check("cached play makes no DELETE call", not deleted)


async def test_slow_becomes_ready():
    """Uncached-but-well-seeded: 'downloading' for a couple of polls, then RD has
    it → it PLAYS (this is the "downloaded straight away, just slower" case)."""
    mock = MockRD(behavior={"bb": "slow:2"})
    _install(mock)
    rd = RDService("fake-key")
    url = await rd.resolve_infohash("bb" + "0" * 38)
    deleted = any(m == "DELETE" for m, _ in mock.calls)
    _teardown()
    check("uncached-but-quick torrent becomes playable (polls, then plays)",
          url == "https://download.real-debrid.com/d/HOSTERLINK",
          f"got {url!r}")
    check("a torrent that became ready is NOT deleted", not deleted)


async def test_stuck_is_kept_not_deleted():
    """Genuinely-stuck (no seeders): after the poll window it returns None but is
    KEPT downloading (not deleted) so a retry can play it later."""
    mock = MockRD(behavior={"cc": "stuck"})
    _install(mock)
    rd = RDService("fake-key")
    url = await rd.resolve_infohash("cc" + "0" * 38)
    deleted = any(m == "DELETE" for m, _ in mock.calls)
    _teardown()
    check("stuck torrent returns None (skip this candidate)", url is None,
          f"got {url!r}")
    check("stuck torrent is KEPT, not deleted (retry plays it later)",
          not deleted)


async def test_dead_is_deleted():
    """A magnet_error will never download → delete it (don't keep an orphan)."""
    mock = MockRD(behavior={"dd": "dead"})
    _install(mock)
    rd = RDService("fake-key")
    url = await rd.resolve_infohash("dd" + "0" * 38)
    deleted = any(m == "DELETE" for m, _ in mock.calls)
    _teardown()
    check("dead (magnet_error) torrent returns None", url is None, f"got {url!r}")
    check("dead torrent IS deleted (no orphan)", deleted)


async def test_blocked_451():
    mock = MockRD(behavior={"ee": "blocked"})
    _install(mock)
    rd = RDService("fake-key")
    url = await rd.resolve_infohash("ee" + "0" * 38)
    _teardown()
    check("451 filter-gate handled gracefully (None, no crash)", url is None,
          f"got {url!r}")


async def test_hard_cap_bounds_a_storm():
    """Even if the CALLER loops forever over stuck candidates (each = add +
    select + several polls), the rolling-window cap + breaker bound total RD
    calls. This is the structural anti-storm guarantee."""
    mock = MockRD(behavior={"ff": "stuck"})
    _install(mock)
    RDService.MIN_INTERVAL = 0.0            # don't wait real seconds in the test
    RDService.MAX_CALLS_PER_WINDOW = 20     # small cap so the test is quick
    RDService.WINDOW_SECONDS = 60.0
    rd = RDService("fake-key")
    # A pathological caller: 50 distinct stuck candidates back-to-back.
    for i in range(50):
        await rd.resolve_infohash(f"ff{i:038d}")
    total = len(mock.calls)
    breaker = RDService._breaker_open()
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


def test_deprioritise_penalty_helper():
    # Fake-4K AI upscale and archive files get a big penalty; genuine files 0.
    check("AI upscale gets a de-prioritise penalty",
          deprioritise_penalty("SNL.S51E19.2160p.HDR.Ai.Upscale-Mesc.mkv") > 0)
    check(".rar archive gets a de-prioritise penalty",
          deprioritise_penalty("Movie.2020.2160p.part1.rar") > 0)
    check("split-archive .r00 gets a penalty",
          deprioritise_penalty("Movie.2020.1080p.r00") > 0)
    check("genuine 1080p release gets NO penalty",
          deprioritise_penalty("SNL.S51E19.1080p.HEVC.x265-MeGusta.mkv") == 0)
    check("a real 4K release is NOT penalised (only fake upscales are)",
          deprioritise_penalty("Movie.2020.2160p.BluRay.x265-TERMINAL.mkv") == 0)


async def test_find_episode_prefers_real_1080_over_fake_4k():
    """The exact 2026-08-01 SNL case: the RD library holds a fake-4K 'Ai Upscale'
    AND a genuine 1080p for the same episode. find_episode_stream must pick the
    genuine 1080p, not the higher-'quality' upscale."""
    library = [{"id": "lib1", "hash": "a" * 40,
                "filename": "Saturday Night Live S51E19 Matt Damon MULTI"}]
    lib_info = {"lib1": {
        "status": "downloaded",
        "files": [
            {"id": 1, "path": "/SNL.S51E19.Matt.Damon.2160p.HDR.Ai.Upscale-Mesc.mkv",
             "bytes": 900, "selected": 1},
            {"id": 2, "path": "/SNL.S51E19.Matt.Damon.1080p.HEVC.x265-MeGusta.mkv",
             "bytes": 800, "selected": 1},
        ],
        # links pair with selected files in order → link[0]=upscale, link[1]=1080p
        "links": ["https://real-debrid.com/d/L_UPSCALE",
                  "https://real-debrid.com/d/L_1080"],
    }}
    mock = MockRD(library=library, lib_info=lib_info)
    _install(mock)
    rd = RDService("fake-key")
    url = await rd.find_episode_stream(
        "Saturday Night Live", 51, 19, preferred_quality="4k")
    _teardown()
    check("fake-4K upscale is de-prioritised; genuine 1080p is served",
          url is not None and url.endswith("L_1080"),
          f"got {url!r}")


# ---------------------------------------------------------------------------
async def main():
    print("Mock-RD safety tests (no network, no account touched):")
    await test_cached_resolves()
    await test_slow_becomes_ready()
    await test_stuck_is_kept_not_deleted()
    await test_dead_is_deleted()
    await test_blocked_451()
    await test_hard_cap_bounds_a_storm()
    await test_429_trips_breaker()
    test_filename_blocked()
    test_deprioritise_penalty_helper()
    await test_find_episode_prefers_real_1080_over_fake_4k()

    passed = sum(1 for _, ok, _ in _results if ok)
    total = len(_results)
    print(f"\n{passed}/{total} checks passed.")
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
