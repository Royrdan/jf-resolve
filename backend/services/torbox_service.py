"""TorBox direct integration.

A drop-in, API-compatible sibling of :class:`RDService` — same three public
entry points (``resolve_infohash``, ``find_episode_stream``,
``find_movie_stream``) and the same ``(api_key)`` constructor — so
``stream.py`` can route to either provider by a single ``debrid_provider``
setting without any other change.

Why TorBox is simpler than RD here:
  * It exposes a real cache-check (``/torrents/checkcached``) that returns the
    torrent's full file list WITHOUT adding anything to the account, so the
    cached-only policy is enforced by a cheap read instead of an add+poll+delete
    dance.
  * It has no May-2026 filename filter-gate, so there is nothing to pre-skip.
  * It is multi-IP and far more forgiving on rate, so this needs only a light
    spacing throttle, not RD's circuit-breaker + rolling-window rails.

All quality-ranking, episode-matching and file-picking logic is provider-
agnostic, so it is reused verbatim from :mod:`rd_service` rather than forked.
"""

import asyncio
import re
import time
from typing import Dict, List, Optional

import httpx

from .log_service import log_service
from .stream_validator import FOREIGN_DUB_RELEASE
from .rd_service import (
    RDService,
    _has_denied_video_codec,
    deprioritise_penalty,
)


class TorBoxService:
    """Resolves torrent infohashes / library items to direct TorBox CDN URLs."""

    BASE_URL = "https://api.torbox.app/v1/api"

    # TorBox is multi-IP and lenient; a light process-wide spacing throttle is
    # plenty (no breaker/window rails like RD needs). One lock shared across
    # instances since every call goes through the same account token.
    _throttle_lock = asyncio.Lock()
    _last_call = 0.0
    MIN_INTERVAL = 0.25  # seconds between TorBox calls

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {"Authorization": f"Bearer {api_key}"}

    # ------------------------------------------------------------------
    # Low-level request choke-point
    # ------------------------------------------------------------------
    async def _request(self, method: str, path: str, **kwargs) -> Optional[httpx.Response]:
        """Single point for every TorBox call: light spacing + transport guard.

        Returns the response (caller inspects status/body) or None on a
        transport-level error.
        """
        kwargs.setdefault("timeout", 15.0)
        kwargs.setdefault("headers", self.headers)
        url = f"{self.BASE_URL}{path}"

        async with TorBoxService._throttle_lock:
            gap = time.monotonic() - TorBoxService._last_call
            if gap < self.MIN_INTERVAL:
                await asyncio.sleep(self.MIN_INTERVAL - gap)
            TorBoxService._last_call = time.monotonic()

        try:
            async with httpx.AsyncClient(verify=False) as client:
                return await client.request(method, url, **kwargs)
        except Exception as e:
            log_service.error(
                f"TorBox: {method} {path} transport error [{type(e).__name__}]: {e}"
            )
            return None

    @staticmethod
    def _envelope(resp: Optional[httpx.Response]):
        """Unwrap TorBox's ``{success,error,detail,data}`` envelope → data|None."""
        if resp is None or resp.status_code not in (200, 201):
            if resp is not None:
                log_service.error(
                    f"TorBox: HTTP {resp.status_code}: {resp.text[:200]}"
                )
            return None
        try:
            body = resp.json()
        except Exception:
            log_service.error("TorBox: non-JSON response body")
            return None
        if not body.get("success"):
            log_service.info(f"TorBox: call not successful: {body.get('detail')}")
            return None
        return body.get("data")

    # ------------------------------------------------------------------
    # TorBox API calls
    # ------------------------------------------------------------------
    async def check_cached(self, infohash: str) -> Optional[Dict]:
        """Return the cached torrent dict (name/size/files) for a hash, or None
        if TorBox does not have it cached. Read-only — adds nothing to the
        account, so this is the cached-only gate."""
        infohash = infohash.lower()
        resp = await self._request(
            "GET",
            "/torrents/checkcached",
            params={"hash": infohash, "format": "object", "list_files": "true"},
        )
        data = self._envelope(resp)
        if not data:
            return None
        # format=object → {hash: {...}}; be tolerant of a bare object too.
        if isinstance(data, dict):
            entry = data.get(infohash) or data.get(infohash.upper())
            if entry:
                return entry
            if data.get("files") or data.get("hash"):
                return data
        return None

    async def check_cached_batch(self, infohashes: List[str]) -> set:
        """Return the SUBSET of ``infohashes`` that TorBox has cached, as a set of
        lowercased hashes. One cheap read per chunk — ``/torrents/checkcached``
        accepts many ``hash=`` params at once, so the whole candidate list is
        vetted in one or two calls instead of one-at-a-time. Read-only: adds
        nothing to the account. Used to reorder cached candidates to the front of
        the resolve walk so a play never dead-ends on a probe cap before it
        reaches an already-cached source."""
        cached: set = set()
        # De-dup, drop falsy, normalise. Chunk to keep the URL well within any
        # gateway limit (40-hex hashes; 50/chunk ≈ 2.3 KB of query string).
        uniq = []
        seen = set()
        for h in infohashes:
            if not h:
                continue
            hl = h.lower()
            if hl not in seen:
                seen.add(hl)
                uniq.append(hl)
        for i in range(0, len(uniq), 50):
            chunk = uniq[i:i + 50]
            resp = await self._request(
                "GET",
                "/torrents/checkcached",
                params=[("hash", h) for h in chunk]
                + [("format", "object"), ("list_files", "false")],
            )
            data = self._envelope(resp)
            if isinstance(data, dict):
                for h in chunk:
                    if data.get(h) or data.get(h.upper()):
                        cached.add(h)
            elif isinstance(data, list):
                # tolerate list-format responses ([{hash: ...}, ...])
                for entry in data:
                    hh = str(entry.get("hash", "")).lower()
                    if hh in seen:
                        cached.add(hh)
        return cached

    async def create_torrent(self, infohash: str) -> Optional[int]:
        """Add a magnet (by infohash) and return its TorBox ``torrent_id``. For
        an already-cached hash this is instant (no download queued)."""
        magnet = f"magnet:?xt=urn:btih:{infohash.lower()}"
        resp = await self._request(
            "POST", "/torrents/createtorrent", data={"magnet": magnet}
        )
        data = self._envelope(resp)
        if isinstance(data, dict):
            return data.get("torrent_id")
        return None

    async def list_files(self, torrent_id: int) -> List[Dict]:
        """Authoritative file list (with ids) for a torrent in the library."""
        resp = await self._request(
            "GET",
            "/torrents/mylist",
            params={"id": torrent_id, "bypass_cache": "true"},
        )
        data = self._envelope(resp)
        if isinstance(data, dict):
            return data.get("files", []) or []
        if isinstance(data, list) and data:
            return data[0].get("files", []) or []
        return []

    async def get_library(self) -> List[Dict]:
        """All torrents in the user's TorBox library."""
        resp = await self._request(
            "GET", "/torrents/mylist", params={"bypass_cache": "true"}
        )
        data = self._envelope(resp)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        return []

    async def request_dl(self, torrent_id: int, file_id: int) -> Optional[str]:
        """Request a direct, range-capable CDN URL for one file.

        Note: requestdl authenticates via a ``token`` query param (not the
        Bearer header) and the returned URL itself carries the token — that is
        TorBox's design; Jellyfin fetches it server-side.
        """
        resp = await self._request(
            "GET",
            "/torrents/requestdl",
            params={
                "token": self.api_key,
                "torrent_id": torrent_id,
                "file_id": file_id,
            },
        )
        data = self._envelope(resp)
        if isinstance(data, str):
            return data
        if isinstance(data, dict):  # tolerate {"url": ...} shape
            return data.get("url") or data.get("download")
        return None

    # ------------------------------------------------------------------
    # File selection (mirrors RDService._pick_link, TorBox file shape)
    # ------------------------------------------------------------------
    @staticmethod
    def _file_name(f: Dict) -> str:
        return f.get("name") or f.get("short_name") or ""

    # Real video containers only. TorBox returns the WHOLE torrent file list
    # (incl. .nfo/.srt/.jpg/sample), and a sidecar like
    # "Show.S01E07.1080p.x265-ELiTE.nfo" matches BOTH the episode pattern and a
    # quality token — so without this guard the picker can hand back a .nfo,
    # which then fails ffprobe. RD's parallel path is shielded by its links
    # array; TorBox needs the extension check explicitly.
    _VIDEO_EXTS = frozenset({
        "mkv", "mp4", "avi", "m4v", "mov", "ts", "m2ts", "wmv",
        "flv", "webm", "mpg", "mpeg", "vob", "divx", "ogm",
    })

    @classmethod
    def _is_video_file(cls, name: str) -> bool:
        ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
        return ext in cls._VIDEO_EXTS

    def _pick_file_id(
        self,
        files: List[Dict],
        season: Optional[int],
        episode: Optional[int],
        filename_hint: Optional[str],
    ) -> Optional[int]:
        """Choose the file id for the requested item. Episode pattern → filename
        hint → single file → largest (movie). Ambiguous TV pack → None (never
        guess an episode)."""
        if not files:
            return None
        # Video files only, then drop extras/samples.
        vids = [f for f in files if self._is_video_file(self._file_name(f))] or files
        candidates = [
            f for f in vids if not RDService._is_extras_path(self._file_name(f))
        ] or vids

        if season is not None and episode is not None:
            ep_pat = RDService._episode_pattern(season, episode)
            for f in candidates:
                if ep_pat.search(self._file_name(f)):
                    return f.get("id")

        if filename_hint:
            hint = RDService._normalise(filename_hint)
            for f in candidates:
                if RDService._normalise(self._file_name(f).split("/")[-1]) == hint:
                    return f.get("id")

        if len(candidates) == 1:
            return candidates[0].get("id")
        if season is not None and episode is not None:
            return None
        return max(candidates, key=lambda f: f.get("size", 0)).get("id")

    # ------------------------------------------------------------------
    # Title-matching helpers (small, provider-agnostic)
    # ------------------------------------------------------------------
    @staticmethod
    def _title_matches(name: str, title_words: List[str]) -> bool:
        norm = RDService._normalise(name)
        compact = norm.replace(" ", "")
        return all(w in norm or w.replace(" ", "") in compact for w in title_words)

    @staticmethod
    def _season_in_name(name: str, season: int) -> bool:
        low = name.lower()
        if "complete" in low:
            return True
        # Accept "S01 13" (dash lost to a space) as well as S01-13 / S01-S13.
        # (?![\dpi]) stops "S01 1080p" being read as a 01-10 range.
        for m in re.finditer(
            r"s(?:eason)?s?\s*0*(\d{1,2})(?:\s*(?:-|–|to)\s*|\s+)"
            r"s?(?:eason)?\s*0*(\d{1,2})(?![\dpi])",
            low,
        ):
            if int(m.group(1)) <= season <= int(m.group(2)):
                return True
        return bool(re.search(rf"s(?:eason)?\s*0*{season}(?!\d)", low))

    # ------------------------------------------------------------------
    # Public: convert an addon-scraped infohash → direct URL (cached-only)
    # ------------------------------------------------------------------
    async def resolve_infohash(
        self,
        infohash: str,
        season: Optional[int] = None,
        episode: Optional[int] = None,
        filename_hint: Optional[str] = None,
    ) -> Optional[str]:
        """Convert a torrent infohash into a stable TorBox direct-download URL.

        Cached-only: if TorBox does not already have the hash cached we return
        None (the caller skips to the next candidate) rather than queue a
        download onto the account. Cached hashes are added instantly and a
        direct CDN link is returned.
        """
        infohash = infohash.lower()

        cached = await self.check_cached(infohash)
        if not cached:
            log_service.info(
                f"TorBox: infohash {infohash[:8]} not cached — skipping candidate."
            )
            return None

        torrent_id = await self.create_torrent(infohash)
        if torrent_id is None:
            log_service.info(
                f"TorBox: createtorrent failed for cached {infohash[:8]}."
            )
            return None

        # Authoritative file ids from the added torrent; fall back to the
        # cache-check listing if mylist is momentarily empty.
        files = await self.list_files(torrent_id) or cached.get("files", [])
        file_id = self._pick_file_id(files, season, episode, filename_hint)
        if file_id is None:
            log_service.info(
                f"TorBox: no file match for {infohash[:8]} "
                f"(S{season}E{episode}) — skipping."
            )
            return None

        url = await self.request_dl(torrent_id, file_id)
        if url:
            log_service.info(
                f"TorBox: resolved {infohash[:8]} file {file_id} → {url[:80]}..."
            )
        return url

    # ------------------------------------------------------------------
    # Public: queue an UNCACHED hash and wait for it to download
    # ------------------------------------------------------------------
    async def torrent_status(self, torrent_id: int) -> Optional[Dict]:
        """Return the full mylist record (download_state/progress/files) for one
        torrent, or None if it isn't listed yet."""
        resp = await self._request(
            "GET",
            "/torrents/mylist",
            params={"id": torrent_id, "bypass_cache": "true"},
        )
        data = self._envelope(resp)
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and data:
            return data[0]
        return None

    @staticmethod
    def _download_ready(rec: Optional[Dict]) -> Optional[bool]:
        """Tri-state readiness for a torrent record:
        True  → downloaded/cached, safe to request a link
        False → dead (error/stalled/no seeds), abandon now
        None  → still in progress, keep polling."""
        if not rec:
            return None
        state = str(rec.get("download_state", "") or "").lower()
        progress = rec.get("progress") or 0
        finished = rec.get("download_finished") or rec.get("cached")
        if finished or (isinstance(progress, (int, float)) and progress >= 1) \
                or any(s in state for s in ("completed", "uploading", "cached")):
            return True
        if any(s in state for s in ("error", "stalled", "missing", "dead", "no seed")):
            return False
        return None

    async def load_and_wait(
        self,
        infohash: str,
        season: Optional[int] = None,
        episode: Optional[int] = None,
        filename_hint: Optional[str] = None,
        wait_budget: float = 20.0,
        poll_interval: float = 2.0,
    ) -> Optional[str]:
        """Queue an UNCACHED infohash onto the account and poll until it has
        finished downloading (or ``wait_budget`` seconds elapse), then return a
        direct URL for the wanted file.

        Unlike :meth:`resolve_infohash` (cached-only), this DELIBERATELY adds a
        download. If it doesn't finish inside the budget we return None but leave
        it downloading server-side, so a later retry serves it from cache. Dead
        magnets (no seeds / error) are abandoned as soon as TorBox reports it, so
        a 0-seed hash doesn't burn the whole budget.
        """
        infohash = infohash.lower()
        torrent_id = await self.create_torrent(infohash)
        if torrent_id is None:
            log_service.info(
                f"TorBox: createtorrent failed for {infohash[:8]} (load)."
            )
            return None

        deadline = time.monotonic() + max(0.0, wait_budget)
        rec = None
        while True:
            rec = await self.torrent_status(torrent_id)
            ready = self._download_ready(rec)
            if ready is True:
                break
            if ready is False:
                log_service.warning(
                    f"TorBox: {infohash[:8]} download not viable "
                    f"(state={(rec or {}).get('download_state')!r}) — abandoning."
                )
                return None
            if time.monotonic() >= deadline:
                pct = int((((rec or {}).get('progress') or 0)) * 100)
                log_service.info(
                    f"TorBox: {infohash[:8]} still downloading ({pct}%) after "
                    f"{wait_budget:.0f}s — leaving it to finish in background."
                )
                return None
            await asyncio.sleep(poll_interval)

        files = await self.list_files(torrent_id) or (rec or {}).get("files", [])
        file_id = self._pick_file_id(files, season, episode, filename_hint)
        if file_id is None:
            log_service.info(
                f"TorBox: {infohash[:8]} downloaded but no file match "
                f"(S{season}E{episode}) — skipping."
            )
            return None

        url = await self.request_dl(torrent_id, file_id)
        if url:
            log_service.info(
                f"TorBox: loaded uncached {infohash[:8]} file {file_id} "
                f"→ {url[:80]}..."
            )
        return url

    # ------------------------------------------------------------------
    # Public: search the user's TorBox library for an episode/movie
    # ------------------------------------------------------------------
    def _score_files(
        self,
        files: List[Dict],
        ep_re: Optional["re.Pattern[str]"],
        pref_rank: float,
        strict_quality: bool,
        block_cam: bool,
        prefer_english: bool,
        min_bytes: int,
    ) -> List[tuple]:
        """Return [(score, file_id, name)] for playable, matching files."""
        matches: List[tuple] = []
        for f in files:
            name = self._file_name(f)
            low = name.lower()
            if not name:
                continue
            if not self._is_video_file(name):  # skip .nfo/.srt/.jpg/sample sidecars
                continue
            if ep_re is not None and not ep_re.search(low):
                continue
            if min_bytes and f.get("size", 0) < min_bytes:
                continue
            if RDService._is_extras_path(low):
                continue
            if _has_denied_video_codec(low):
                continue
            if prefer_english and FOREIGN_DUB_RELEASE.search(low):
                continue
            q_rank = RDService._quality_rank(low)
            if block_cam and q_rank == 0.5:
                continue
            if strict_quality and pref_rank > 0 and q_rank > 0 and q_rank != pref_rank:
                continue
            score = 10 - abs(q_rank - pref_rank) - deprioritise_penalty(low)
            matches.append((score, f.get("id"), name))
        return matches

    async def _resolve_from_matches(
        self,
        matches_by_torrent: List[tuple],
        pref_rank: float,
        strict_quality: bool,
        use_index: int,
    ) -> Optional[str]:
        """Shared tail for find_*: quality-floor, clamp, then requestdl walk."""
        matches = sorted(matches_by_torrent, key=lambda x: x[0], reverse=True)
        if not matches:
            return None

        # Quality floor (auto only): don't short-circuit playback with a copy
        # more than one tier below the preference — defer to the Stremio walk.
        if not strict_quality and pref_rank > 0:
            best_rank = RDService._quality_rank(matches[0][3])
            if best_rank > 0 and (pref_rank - best_rank) > 1.0:
                log_service.info(
                    f"TorBox: best library match {matches[0][3]} is >1 tier below "
                    f"preferred — deferring to Stremio for a better source."
                )
                return None

        start = use_index if use_index < len(matches) else 0
        order = [start] + [i for i in range(len(matches)) if i != start]
        for i in order:
            _score, torrent_id, file_id, name = matches[i]
            url = await self.request_dl(torrent_id, file_id)
            if url:
                log_service.info(f"TorBox: library hit → {name} → {url[:80]}...")
                return url
        return None

    async def find_episode_stream(
        self,
        show_title: str,
        season: int,
        episode: int,
        preferred_quality: str = "1080p",
        use_index: int = 0,
        strict_quality: bool = False,
        block_cam: bool = True,
        prefer_english: bool = True,
    ) -> Optional[str]:
        """Search the user's TorBox library for a specific TV episode."""
        ep_re = RDService._episode_pattern(season, episode)
        title_words = [w for w in RDService._normalise(show_title).split() if len(w) > 1]
        pref_rank = RDService._preferred_rank(preferred_quality)

        library = await self.get_library()
        candidates = [
            t for t in library
            if self._title_matches(t.get("name", ""), title_words)
            and self._season_in_name(t.get("name", ""), season)
        ]
        if not candidates:
            log_service.info(
                f"TorBox: no library matches for '{show_title}' "
                f"S{season:02d}E{episode:02d}"
            )
            return None

        scored: List[tuple] = []
        for t in candidates:
            files = t.get("files") or await self.list_files(t.get("id"))
            for s, fid, name in self._score_files(
                files, ep_re, pref_rank, strict_quality, block_cam,
                prefer_english, min_bytes=0,
            ):
                scored.append((s, t.get("id"), fid, name))

        return await self._resolve_from_matches(
            scored, pref_rank, strict_quality, use_index
        )

    async def find_movie_stream(
        self,
        movie_title: str,
        year: Optional[int],
        preferred_quality: str = "1080p",
        use_index: int = 0,
        strict_quality: bool = False,
        block_cam: bool = True,
        prefer_english: bool = True,
    ) -> Optional[str]:
        """Search the user's TorBox library for a movie file."""
        title_words = [w for w in RDService._normalise(movie_title).split() if len(w) > 1]
        pref_rank = RDService._preferred_rank(preferred_quality)

        library = await self.get_library()
        candidates = [
            t for t in library
            if self._title_matches(t.get("name", ""), title_words)
            and (not year or str(year) in RDService._normalise(t.get("name", "")))
        ]
        if not candidates:
            log_service.info(
                f"TorBox: no library matches for movie '{movie_title}' ({year})"
            )
            return None

        scored: List[tuple] = []
        for t in candidates:
            files = t.get("files") or await self.list_files(t.get("id"))
            for s, fid, name in self._score_files(
                files, None, pref_rank, strict_quality, block_cam,
                prefer_english, min_bytes=100 * 1024 * 1024,
            ):
                scored.append((s, t.get("id"), fid, name))

        return await self._resolve_from_matches(
            scored, pref_rank, strict_quality, use_index
        )
