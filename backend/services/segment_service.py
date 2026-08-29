"""Intro / credits (media segment) detection for debrid streams.

Jellyfin's Intro-Skipper analyses LOCAL library files; our content is remote
debrid, so nothing on the server can look at it. But jf-resolve already holds
the resolved, range-capable direct URL for every episode — so we reproduce the
proven Intro-Skipper approach here instead: Chromaprint audio fingerprinting.

Method (validated on real debrid episodes):
  * Fingerprint the HEAD window (first few minutes) of the target episode and a
    reference episode, then cross-correlate the two fingerprints. The longest
    run of low-Hamming-distance matches is the recurring theme = the INTRO.
  * Do the same on the TAIL window to find the recurring end-theme = CREDITS.
Audio decodes cheaply and only the head/tail windows are pulled (range requests),
so this barely touches debrid bandwidth and never blocks playback (callers run
it in the background / at pre-warm time and cache the result).

The core functions depend only on stdlib + httpx + ffmpeg/fpcalc, so this module
can be run standalone for testing:
    python -m backend.services.segment_service --imdb tt0182576 --tmdb 1434 -s 2 -e 1
"""

import argparse
import asyncio
import json
import os
import subprocess
import tempfile
from dataclasses import dataclass, asdict
from typing import List, Optional, Tuple

import httpx

try:  # integrated: use the app logger. standalone: fall back to print.
    from .log_service import log_service
except Exception:  # pragma: no cover - standalone

    class _PrintLog:
        def info(self, m):
            print("INFO ", m)

        def warning(self, m):
            print("WARN ", m)

    log_service = _PrintLog()


# --- tunables -------------------------------------------------------------
HEAD_SEC = 300           # fingerprint the first 5 min for the intro
TAIL_SEC = 240           # fingerprint the last 4 min for the credits
HAMMING_THRESH = 6       # max differing bits (of 32) to call two hashes a match
MAX_SHIFT_SEC = 90       # how far the recurring segment may shift between eps
MIN_INTRO_SEC = 10.0     # ignore matches shorter than this (noise)
MIN_CREDITS_SEC = 8.0
RESOLVE_BASE = os.environ.get("JFRESOLVE_INTERNAL_URL", "http://localhost:8766")


@dataclass
class Segment:
    start: float
    end: float

    def as_ticks(self) -> dict:
        # Jellyfin media-segment ticks are 100-ns units.
        return {"start_ticks": int(self.start * 1e7), "end_ticks": int(self.end * 1e7)}


@dataclass
class DetectResult:
    duration: Optional[float] = None
    intro: Optional[Segment] = None
    credits: Optional[Segment] = None
    method: str = "chromaprint"
    reference: Optional[str] = None  # which episode was used as the reference

    def to_dict(self) -> dict:
        return {
            "duration": self.duration,
            "intro": asdict(self.intro) if self.intro else None,
            "credits": asdict(self.credits) if self.credits else None,
            "method": self.method,
            "reference": self.reference,
        }


def _popcount(x: int) -> int:
    return bin(x & 0xFFFFFFFF).count("1")


def _correlate(a: List[int], b: List[int], hps: float,
               min_run_sec: float) -> Optional[Tuple[float, float, float]]:
    """Slide fingerprint b against a; return the longest low-Hamming run as
    (run_start_in_a_sec, run_len_sec, shift_sec), or None if nothing solid.

    a, b are raw Chromaprint 32-bit hash lists. hps = hashes per second.
    """
    if not a or not b:
        return None
    na, nb = len(a), len(b)
    max_shift = int(MAX_SHIFT_SEC * hps)
    best_run = 0
    best_off = 0
    best_start = 0
    for off in range(-max_shift, max_shift + 1):
        run = 0
        run_start = 0
        lo = max(0, off)
        hi = min(na, nb + off)
        for i in range(lo, hi):
            if _popcount(a[i] ^ b[i - off]) <= HAMMING_THRESH:
                if run == 0:
                    run_start = i
                run += 1
                if run > best_run:
                    best_run = run
                    best_off = off
                    best_start = run_start
            else:
                run = 0
    run_sec = best_run / hps
    if run_sec < min_run_sec:
        return None
    return (best_start / hps, run_sec, best_off / hps)


async def _resolve_url(client: httpx.AsyncClient, media_type: str, tmdb_id: str,
                       season: Optional[int], episode: Optional[int],
                       imdb_id: Optional[str], quality: str, index: int) -> Optional[str]:
    """Hit our own resolve endpoint (no-redirect) and read the direct CDN URL
    from the 302 Location. Reuses jf-resolve's cache + coalescing."""
    params = {"quality": quality, "index": index}
    if season is not None:
        params["season"] = season
    if episode is not None:
        params["episode"] = episode
    if imdb_id:
        params["imdb_id"] = imdb_id
    url = f"{RESOLVE_BASE}/api/stream/resolve/{media_type}/{tmdb_id}"
    try:
        r = await client.get(url, params=params, follow_redirects=False, timeout=90)
    except Exception as e:  # noqa: BLE001
        log_service.warning(f"segment resolve failed {media_type}/{tmdb_id} s{season}e{episode}: {e}")
        return None
    if r.status_code in (301, 302, 303, 307, 308):
        return r.headers.get("location")
    log_service.warning(f"segment resolve non-redirect {r.status_code} for s{season}e{episode}")
    return None


async def _fpcalc_head(url: str, length: int) -> Tuple[Optional[List[int]], float]:
    """Chromaprint the first `length` seconds of the URL directly (fpcalc
    seeks from 0). Returns (hashes, hashes_per_second)."""
    proc = await asyncio.create_subprocess_exec(
        "fpcalc", "-raw", "-length", str(length), url,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=length + 60)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        return None, 0.0
    return _parse_fpcalc(out.decode("utf-8", "replace"), length)


async def _fpcalc_tail(url: str, duration: float, tail: int) -> Tuple[Optional[List[int]], float, float]:
    """Chromaprint the last `tail` seconds. fpcalc can't seek, so extract the
    tail audio with ffmpeg first. Returns (hashes, hps, tail_start_sec)."""
    ss = max(0.0, duration - tail)
    tmp = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
    tmp.close()
    try:
        p = await asyncio.create_subprocess_exec(
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
            "-ss", str(ss), "-i", url, "-ac", "1", "-ar", "11025",
            "-f", "wav", tmp.name,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        try:
            await asyncio.wait_for(p.communicate(), timeout=tail + 90)
        except asyncio.TimeoutError:
            try:
                p.kill()
            except ProcessLookupError:
                pass
            return None, 0.0, ss
        # -length is REQUIRED: without it fpcalc analyses only the first 120s of
        # the wav, dropping the credits that sit at the very end of a longer tail.
        fp = await asyncio.create_subprocess_exec(
            "fpcalc", "-raw", "-length", str(tail), tmp.name,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        out, _ = await asyncio.wait_for(fp.communicate(), timeout=120)
        hashes, hps = _parse_fpcalc(out.decode("utf-8", "replace"), tail)
        return hashes, hps, ss
    finally:
        try:
            os.unlink(tmp.name)
        except OSError:
            pass


def _parse_fpcalc(text: str, analyzed_sec: int) -> Tuple[Optional[List[int]], float]:
    for line in text.splitlines():
        if line.startswith("FINGERPRINT="):
            hashes = [int(x) for x in line[len("FINGERPRINT="):].split(",") if x]
            hps = (len(hashes) / analyzed_sec) if analyzed_sec else 8.0
            return hashes, hps
    return None, 0.0


async def _duration(url: str) -> Optional[float]:
    proc = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "error", "-show_entries", "format=duration",
        "-of", "csv=p=0", url,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=120)
        return float(out.decode().strip())
    except Exception:  # noqa: BLE001
        return None


def _pick_reference(episode: Optional[int]) -> Optional[int]:
    """Adjacent episode used as the fingerprint reference (next, else previous)."""
    if episode is None:
        return None
    return episode + 1 if episode >= 1 else None


async def detect(media_type: str, tmdb_id: str, imdb_id: Optional[str] = None,
                 season: Optional[int] = None, episode: Optional[int] = None,
                 quality: str = "auto", index: int = 0,
                 reference_episode: Optional[int] = None) -> DetectResult:
    """Detect intro + credits for one episode by cross-correlating its head/tail
    fingerprints against an adjacent reference episode. TV only for now."""
    result = DetectResult()
    if media_type != "tv" or episode is None:
        log_service.info("segment detect: movies/non-episodic not supported yet (needs reference)")
        return result

    ref_ep = reference_episode or _pick_reference(episode)
    async with httpx.AsyncClient() as client:
        target_url = await _resolve_url(client, media_type, tmdb_id, season, episode, imdb_id, quality, index)
        if not target_url:
            return result
        ref_url = None
        if ref_ep is not None:
            ref_url = await _resolve_url(client, media_type, tmdb_id, season, ref_ep, imdb_id, quality, index)
        # fall back to previous episode if next didn't resolve
        if not ref_url and episode and episode > 1:
            ref_ep = episode - 1
            ref_url = await _resolve_url(client, media_type, tmdb_id, season, ref_ep, imdb_id, quality, index)
        if not ref_url:
            log_service.info(f"segment detect: no reference episode resolvable for s{season}e{episode}")
            return result
        result.reference = f"s{season}e{ref_ep}"

    dur = await _duration(target_url)
    result.duration = dur

    # INTRO — head fingerprints
    a_head, hps_a = await _fpcalc_head(target_url, HEAD_SEC)
    b_head, _ = await _fpcalc_head(ref_url, HEAD_SEC)
    corr = _correlate(a_head, b_head, hps_a, MIN_INTRO_SEC) if a_head and b_head else None
    if corr:
        start, length, _shift = corr
        result.intro = Segment(round(start, 2), round(start + length, 2))
        log_service.info(f"segment INTRO s{season}e{episode}: {result.intro.start:.1f}-{result.intro.end:.1f}s")

    # CREDITS — tail fingerprints (only if we know the duration)
    if dur:
        a_tail, hps_t, ss_a = await _fpcalc_tail(target_url, dur, TAIL_SEC)
        b_tail, _, _ = await _fpcalc_tail(ref_url, (await _duration(ref_url)) or dur, TAIL_SEC)
        corr = _correlate(a_tail, b_tail, hps_t, MIN_CREDITS_SEC) if a_tail and b_tail else None
        if corr:
            start, length, _shift = corr
            cs = ss_a + start
            result.credits = Segment(round(cs, 2), round(cs + length, 2))
            log_service.info(f"segment CREDITS s{season}e{episode}: {result.credits.start:.1f}-{result.credits.end:.1f}s (ends {dur:.0f})")

    return result


async def _main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--imdb")
    ap.add_argument("--tmdb", required=True)
    ap.add_argument("--type", default="tv")
    ap.add_argument("-s", "--season", type=int)
    ap.add_argument("-e", "--episode", type=int)
    ap.add_argument("--ref", type=int, help="reference episode override")
    args = ap.parse_args()
    res = await detect(args.type, args.tmdb, imdb_id=args.imdb, season=args.season,
                       episode=args.episode, reference_episode=args.ref)
    print(json.dumps(res.to_dict(), indent=2))


if __name__ == "__main__":
    asyncio.run(_main())
