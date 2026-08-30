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
MAX_REFS_TRIED = 3       # how many reference episodes to try before giving up
STRONG_INTRO_SEC = 20.0  # a match this long is "confident" — stop trying more refs
STRONG_CREDITS_SEC = 15.0
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
               min_run_sec: float,
               max_shift_sec: float = MAX_SHIFT_SEC) -> Optional[Tuple[float, float, float]]:
    """Slide fingerprint b against a; return the longest low-Hamming run as
    (run_start_in_a_sec, run_len_sec, shift_sec), or None if nothing solid.

    a, b are raw Chromaprint 32-bit hash lists. hps = hashes per second. When b is
    a short stored template, use a large max_shift_sec so it can be found anywhere
    in a (the episode's head/tail window).
    """
    if not a or not b:
        return None
    na, nb = len(a), len(b)
    max_shift = int(max_shift_sec * hps)
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


def _consensus(matches: List[Tuple[float, float]], min_len: float) -> Optional[Tuple[float, float]]:
    """Pick a trustworthy segment from per-reference matches.

    ``matches`` is a list of (start, length) — one candidate per reference episode.
    A real intro/credits recurs at the SAME place across references, so we cluster
    by start time and take the location the most references agree on. A lone
    spurious match (one dud reference lighting up a random recurring cue) is only
    accepted if it's very strong. Returns (start, length) or None.
    """
    good = [(s, l) for s, l in matches if l >= min_len]
    if not good:
        return None
    best_cluster: List[Tuple[float, float]] = []
    for s, _ in good:
        cluster = [(s2, l2) for s2, l2 in good if abs(s2 - s) <= 8.0]
        if len(cluster) > len(best_cluster):
            best_cluster = cluster
    if len(best_cluster) >= 2:
        starts = sorted(x[0] for x in best_cluster)
        return (starts[len(starts) // 2], max(x[1] for x in best_cluster))
    # single match — trust only if clearly strong (>= 2x the minimum)
    s, l = max(good, key=lambda x: x[1])
    return (s, l) if l >= min_len * 2 else None


def _best_match(matches: List[Tuple[float, float]], min_len: float) -> Optional[Tuple[float, float]]:
    """Longest match above ``min_len`` (best-effort — used for credits, where debrid
    release variance means references often don't agree well enough for consensus)."""
    good = [(s, l) for s, l in matches if l >= min_len]
    return max(good, key=lambda x: x[1]) if good else None


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


async def _load_template(key: str) -> Tuple[Optional[List[int]], Optional[List[int]]]:
    """Return (intro_fp, credits_fp) stored for this season, or (None, None)."""
    from ..database import AsyncSessionLocal
    from ..models.season_fingerprint import SeasonFingerprint
    try:
        async with AsyncSessionLocal() as db:
            row = await db.get(SeasonFingerprint, key)
            if not row:
                return (None, None)
            intro = json.loads(row.intro_fp) if row.intro_fp else None
            credits = json.loads(row.credits_fp) if row.credits_fp else None
            return (intro, credits)
    except Exception as e:  # noqa: BLE001
        log_service.warning(f"load template {key} failed: {e}")
        return (None, None)


async def _save_template(key: str, intro: Optional[List[int]] = None,
                         credits: Optional[List[int]] = None) -> None:
    """Upsert a season template, only writing the fields provided."""
    from ..database import AsyncSessionLocal
    from ..models.season_fingerprint import SeasonFingerprint
    try:
        async with AsyncSessionLocal() as db:
            row = await db.get(SeasonFingerprint, key)
            if row is None:
                row = SeasonFingerprint(key=key)
                db.add(row)
            if intro is not None:
                row.intro_fp = json.dumps(intro)
            if credits is not None:
                row.credits_fp = json.dumps(credits)
            await db.commit()
        log_service.info(f"saved template {key} (intro={intro is not None}, credits={credits is not None})")
    except Exception as e:  # noqa: BLE001
        log_service.warning(f"save template {key} failed: {e}")


async def _learn_templates(client, media_type, tmdb_id, season, episode, imdb_id, quality, index,
                           a_head, hps_h, a_tail, hps_t, ss_a) -> dict:
    """Cross-correlate the target against a few reference episodes to locate the
    recurring intro/credits, and slice the target's fingerprint at those spots to
    become the reusable season template. Returns {intro:(start,len,fp), credits:(...)}."""
    candidates = [e for e in (episode + 1, episode - 1, episode + 2, episode - 2) if e and e >= 1]
    intro_matches: List[Tuple[float, float]] = []
    credits_matches: List[Tuple[float, float]] = []
    tried = 0
    for ref_ep in candidates:
        if tried >= MAX_REFS_TRIED:
            break
        ref_url = await _resolve_url(client, media_type, tmdb_id, season, ref_ep, imdb_id, quality, index)
        if not ref_url:
            continue
        tried += 1
        if a_head:
            b, _ = await _fpcalc_head(ref_url, HEAD_SEC)
            c = _correlate(a_head, b, hps_h, MIN_INTRO_SEC) if b else None
            if c:
                intro_matches.append((c[0], c[1]))
        if a_tail:
            rdur = await _duration(ref_url) or (ss_a + TAIL_SEC)
            b, _, _ = await _fpcalc_tail(ref_url, rdur, TAIL_SEC)
            c = _correlate(a_tail, b, hps_t, MIN_CREDITS_SEC) if b else None
            if c:
                credits_matches.append((c[0], c[1]))
        if (_consensus(intro_matches, MIN_INTRO_SEC) and len(intro_matches) >= 2
                and (_consensus(credits_matches, MIN_CREDITS_SEC) and len(credits_matches) >= 2)):
            break

    out: dict = {}
    intro = _consensus(intro_matches, MIN_INTRO_SEC)
    if intro and a_head:
        s, l = intro
        fp = a_head[int(s * hps_h): int((s + l) * hps_h)]
        if fp:
            out["intro"] = (s, l, fp)
    credits = _consensus(credits_matches, MIN_CREDITS_SEC) or _best_match(credits_matches, MIN_CREDITS_SEC * 1.5)
    if credits and a_tail:
        s, l = credits
        fp = a_tail[int(s * hps_t): int((s + l) * hps_t)]
        if fp:
            out["credits"] = (ss_a + s, l, fp)
    return out


async def detect(media_type: str, tmdb_id: str, imdb_id: Optional[str] = None,
                 season: Optional[int] = None, episode: Optional[int] = None,
                 quality: str = "auto", index: int = 0,
                 reference_episode: Optional[int] = None) -> DetectResult:
    """Detect intro + credits for one episode's specific stream.

    Debrid returns a different release per stream, so we don't compare episodes to
    each other directly. Instead we keep a stable per-SEASON fingerprint template
    (the intro/credits theme audio) and locate it inside this stream's head/tail.
    The template is learned once (via cross-episode consensus) and reused. Black
    detection covers credits when no audio template matches. TV only for now.
    """
    result = DetectResult()
    if media_type != "tv" or episode is None:
        log_service.info("segment detect: movies/non-episodic not supported yet")
        return result

    key = f"tv:{tmdb_id}:{season}"
    async with httpx.AsyncClient() as client:
        target_url = await _resolve_url(client, media_type, tmdb_id, season, episode, imdb_id, quality, index)
        if not target_url:
            return result
        dur = await _duration(target_url)
        result.duration = dur

        # Fingerprint this stream's head + tail once.
        a_head, hps_h = await _fpcalc_head(target_url, HEAD_SEC)
        a_tail, hps_t, ss_a = (None, 0.0, 0.0)
        if dur:
            a_tail, hps_t, ss_a = await _fpcalc_tail(target_url, dur, TAIL_SEC)

        intro = None    # (start_sec, len_sec)
        credits = None
        methods = []

        # 1. Match against the stored season template (stable across releases).
        intro_tpl, credits_tpl = await _load_template(key)
        if intro_tpl and a_head:
            c = _correlate(a_head, intro_tpl, hps_h, MIN_INTRO_SEC, max_shift_sec=HEAD_SEC)
            if c:
                intro = (c[0], c[1]); methods.append("intro:template")
        if credits_tpl and a_tail:
            c = _correlate(a_tail, credits_tpl, hps_t, MIN_CREDITS_SEC, max_shift_sec=TAIL_SEC)
            if c:
                credits = (ss_a + c[0], c[1]); methods.append("credits:template")

        # 2. Anything still missing → learn it from cross-episode consensus + store the template.
        if (intro is None and intro_tpl is None) or (credits is None and credits_tpl is None):
            learned = await _learn_templates(client, media_type, tmdb_id, season, episode,
                                             imdb_id, quality, index, a_head, hps_h, a_tail, hps_t, ss_a)
            if intro is None and "intro" in learned:
                s, l, fp = learned["intro"]
                intro = (s, l); await _save_template(key, intro=fp); methods.append("intro:learned")
            if credits is None and "credits" in learned:
                cs, l, fp = learned["credits"]
                credits = (cs, l); await _save_template(key, credits=fp); methods.append("credits:learned")

        # 3. Black-frame fallback for credits (works when audio detection can't).
        if credits is None and dur:
            bd = await _blackdetect_credits(target_url, dur)
            if bd is not None:
                credits = (bd, max(1.0, dur - bd)); methods.append("credits:black")

    result.method = ",".join(methods) or "none"
    if intro:
        s, l = intro
        result.intro = Segment(round(s, 2), round(s + l, 2))
        log_service.info(f"segment INTRO s{season}e{episode}: {result.intro.start:.1f}-{result.intro.end:.1f}s")
    if credits:
        s, l = credits
        result.credits = Segment(round(s, 2), round(s + l, 2))
        log_service.info(f"segment CREDITS s{season}e{episode}: {result.credits.start:.1f}-{result.credits.end:.1f}s")

    return result


async def _blackdetect_credits(url: str, dur: float) -> Optional[float]:
    """Best-effort credits start via black-frame detection on the tail. Scans the
    last few minutes (downscaled, low fps) and returns the start of a sustained
    dark stretch in the final stretch, or None. Noisy for rapid-cut animation;
    reliable for movies / fade-to-black endings."""
    ss = max(0.0, dur - 300.0)
    cmd = ["ffmpeg", "-hide_banner", "-ss", str(ss), "-i", url, "-an",
           "-vf", "fps=4,scale=128:72,blackdetect=d=1.0:pix_th=0.10", "-f", "null", "-"]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        _, err = await asyncio.wait_for(proc.communicate(), timeout=300)
    except Exception:  # noqa: BLE001
        return None
    import re as _re
    blacks = _re.findall(r"black_start:([\d.]+)", err.decode("utf-8", "replace"))
    if not blacks:
        return None
    # Prefer the earliest sustained black that sits in the final ~20% of the runtime
    # (credits region), so mid-episode fades don't trigger it.
    tail_cut = dur * 0.80
    cands = [ss + float(t) for t in blacks if ss + float(t) >= tail_cut]
    return min(cands) if cands else None


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
