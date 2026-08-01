"""Real-Debrid direct library integration"""

import asyncio
import re
import time
from typing import Dict, List, Optional

import httpx

from .log_service import log_service
from .stream_validator import DEFAULT_VIDEO_DENYLIST, FOREIGN_DUB_RELEASE


def _has_denied_video_codec(name: str) -> bool:
    """True if a library filename advertises a denylisted video codec (e.g. av1).

    The RD-direct path selects a file by its library name and never ffprobes it,
    so a codec the household's players can't decode (AV1) has to be excluded here
    by name — otherwise it gets unrestricted and served straight to the client,
    which then freezes. Matched as a whole token so 'av1' can't false-match
    inside another word.
    """
    if not DEFAULT_VIDEO_DENYLIST:
        return False
    lowered = name.lower()
    return any(
        re.search(rf'(?<![a-z0-9]){re.escape(c.lower())}(?![a-z0-9])', lowered)
        for c in DEFAULT_VIDEO_DENYLIST
    )


# Shared cam / theatrical-rip / screener detector. Matches camcorder rips
# (CAM/HDCAM/HQCAM), telesyncs (TS/HDTS/TELESYNC), telecines (TC/HDTC/TELECINE),
# digital-cinema rips (DCP/DCPRiP), pre-retail (PDVD/PreDVD), screeners
# (SCR/SCREENER/DVDSCR), mic-dubs (MD) and Korean-subbed cams (KORSUB).
# These are all below-retail sources we never want to auto-serve.
CAM_PATTERN = re.compile(
    r'\b(cam|camrip|hdcam|hqcam|ts|hdts|telesync|tc|hdtc|telecine|'
    r'scr|screener|dvdscr|dcp|dcprip|pdvd|predvd|korsub|md)\b'
)


# Release groups / tags RD's May-2026 "filter-gate" hard-blocks at addMagnet time
# (it returns 451 infringing_file). Attempting them just burns rate budget and
# walks us deeper into the candidate list, so we skip them BEFORE spending a
# probe. Conservative default (well-reported groups only, not broad source tags
# like WEB-DL which would gut the pool); tune via the `rd_blocked_release_tags`
# DB setting. Matched as whole tokens so 'yts' can't match inside another word.
RD_BLOCKED_RELEASE_TAGS = ["yts", "yify", "rarbg", "galaxyrg"]


# Fake-quality and non-video releases we don't want auto-picked over a genuine
# source. NOT hard-skipped — heavily penalised so they sort to the bottom and
# only ever serve as a last resort (an archive then just fails ffprobe cleanly,
# and a fake-4K upscale loses to a real 1080p). Covers the 2026-08-01 SNL case:
# a "2160p HDR Ai Upscale" release that unrestricted to a .rar.
UPSCALE_PATTERN = re.compile(r"\b(?:ai[\s._\-]*)?upscal(?:e|ed|ing)\b", re.IGNORECASE)
ARCHIVE_EXT_PATTERN = re.compile(
    r"\.(?:rar|zip|7z|tar|gz|bz2|r\d{2,3}|z\d{2}|\d{3})$", re.IGNORECASE
)
DEPRIORITISE_PENALTY = 20.0  # larger than the whole real-score spread (~6-10)


def deprioritise_penalty(name: str) -> float:
    """Score penalty that pushes fake-quality (AI upscale) and non-video
    (archive) files to the bottom of the match ranking without excluding them."""
    if not name:
        return 0.0
    pen = 0.0
    if UPSCALE_PATTERN.search(name):
        pen += DEPRIORITISE_PENALTY
    if ARCHIVE_EXT_PATTERN.search(name):
        pen += DEPRIORITISE_PENALTY
    return pen


def rd_filename_blocked(name: str, tags: Optional[List[str]] = None) -> bool:
    """True if the name carries a release tag RD is known to reject at addMagnet."""
    tag_list = RD_BLOCKED_RELEASE_TAGS if tags is None else tags
    if not name or not tag_list:
        return False
    lowered = name.lower()
    return any(
        re.search(rf'(?<![a-z0-9]){re.escape(t.lower())}(?![a-z0-9])', lowered)
        for t in tag_list
    )


class RDService:
    """
    Queries the user's own Real-Debrid torrent library to find cached files
    and returns unrestricted direct-download URLs without going through
    any Stremio addon.
    """

    BASE_URL = "https://api.real-debrid.com/rest/1.0"
    CACHE_TTL = 300        # 5 minutes — torrent list
    INFO_CACHE_TTL = 3600  # 60 minutes — per-torrent file info

    # Class-level cache keyed by api_key: (timestamp, torrent_list)
    _cache: Dict[str, tuple] = {}
    # Per-torrent info cache keyed by torrent_id: (timestamp, info_dict)
    _info_cache: Dict[str, tuple] = {}

    # Process-wide RD API throttle. Real-Debrid rate-limits per ACCOUNT (token),
    # and this addon is the sole caller, so ONE limiter shared across every
    # RDService instance is correct. Every RD call goes through _request(), which
    # serialises on _throttle_lock and enforces MIN_INTERVAL spacing so bursts
    # (many parallel episode resolves, cache-cold replays, debugging storms)
    # can't hammer RD — that burst behaviour previously earned the account an
    # abuse warning + forced token rotation.
    _throttle_lock = asyncio.Lock()
    _last_call = 0.0
    MIN_INTERVAL = 0.6     # seconds between RD API calls (~100/min, well under cap)
    MAX_RETRIES = 2        # 429 backoff attempts before the breaker trips

    # --- Hard safety rails (added after the 2026-08-01 single-IP storm) ---------
    # RD allows 250 req/min per account and BLOCKS accounts that brute-force past
    # 429 ("bruteforcing will leave you blocked for undefined amount of time" —
    # api.real-debrid.com). Spacing alone isn't enough: a cache-cold play that
    # walks many magnet candidates (each = addMagnet+info+select+delete) can still
    # fire dozens of calls in a burst, and RD's May-2026 filter-gate 451s make it
    # walk even further. Two rails make a storm structurally impossible:
    #   1. Rolling-window hard cap — at most MAX_CALLS_PER_WINDOW calls per
    #      WINDOW_SECONDS across the WHOLE process. Hitting it trips the breaker.
    #   2. Circuit breaker — the first time RD 429s past MAX_RETRIES (or we hit our
    #      own cap), EVERY RD call short-circuits to None for CIRCUIT_COOLDOWN
    #      seconds, so a play fails gracefully instead of hammering the account.
    WINDOW_SECONDS = 60.0
    MAX_CALLS_PER_WINDOW = 45       # a single play needs ~5-20; far under RD's 250
    CIRCUIT_COOLDOWN = 120.0        # after a 429/cap trip, pause ALL RD calls
    _call_times: List[float] = []   # monotonic timestamps of recent calls
    _circuit_open_until = 0.0       # monotonic deadline; > now ⇒ breaker open
    _transport = None               # test hook: httpx transport override (mock RD)

    # Uncached torrents: RD must pull the torrent before it's playable. Popular,
    # well-seeded releases flip to "downloaded" in 1-2s ("downloaded straight
    # away"), so give RD a short BOUNDED window to finish instead of skipping the
    # instant we look. A torrent that's still not ready after the window is KEPT
    # (not deleted) so it keeps downloading and the next press plays it — the
    # "just slower" experience, not a hard fail. Each poll is one get_torrent_info;
    # the rate cap + per-play probe budget bound total volume across candidates.
    POLL_ATTEMPTS = 3               # status re-checks after the first read
    POLL_INTERVAL = 2.5             # seconds between polls → ~8s ready window
    # Statuses that will never become playable → delete rather than keep.
    DEAD_STATUSES = frozenset({"magnet_error", "error", "virus", "dead"})

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {"Authorization": f"Bearer {api_key}"}

    # --- breaker helpers -------------------------------------------------------
    @classmethod
    def _breaker_open(cls) -> bool:
        """True while the circuit breaker is tripped (all RD calls paused)."""
        return time.monotonic() < cls._circuit_open_until

    @classmethod
    def _trip_breaker(cls, reason: str) -> None:
        cls._circuit_open_until = time.monotonic() + cls.CIRCUIT_COOLDOWN
        log_service.error(
            f"RD: circuit breaker OPEN for {cls.CIRCUIT_COOLDOWN:.0f}s ({reason}). "
            f"All Real-Debrid calls short-circuit until it closes."
        )

    @classmethod
    def _reset_rails(cls) -> None:
        """Clear all rate/breaker state — for tests and manual recovery."""
        cls._call_times = []
        cls._circuit_open_until = 0.0
        cls._last_call = 0.0

    async def _request(self, method: str, url: str, **kwargs) -> Optional[httpx.Response]:
        """Single choke-point for every Real-Debrid API call: process-wide rate
        limiting + exponential backoff on HTTP 429. Returns the response (which
        may still carry a non-200 status for the caller to handle) or None on a
        transport-level error."""
        kwargs.setdefault("timeout", 10.0)
        kwargs.setdefault("headers", self.headers)
        endpoint = url.split(self.BASE_URL)[-1] or url

        # Rail 2 (breaker): once RD has pushed back, stop entirely for a cooldown
        # rather than keep hammering — this is what turns a storm into a graceful
        # failure.
        if RDService._breaker_open():
            log_service.warning(
                f"RD: breaker open — skipping {method} {endpoint} (cooling down)."
            )
            return None

        resp = None
        for attempt in range(self.MAX_RETRIES + 1):
            # Rail 1 (rate): spacing + rolling-window hard cap, under one lock so
            # concurrent resolves queue instead of bursting. A call is counted
            # BEFORE it's made; overflowing the window trips the breaker.
            async with RDService._throttle_lock:
                now = time.monotonic()
                RDService._call_times = [
                    t for t in RDService._call_times
                    if now - t < RDService.WINDOW_SECONDS
                ]
                if len(RDService._call_times) >= RDService.MAX_CALLS_PER_WINDOW:
                    RDService._trip_breaker(
                        f"self-imposed cap {RDService.MAX_CALLS_PER_WINDOW}/"
                        f"{RDService.WINDOW_SECONDS:.0f}s reached"
                    )
                    return None
                gap = now - RDService._last_call
                if gap < self.MIN_INTERVAL:
                    await asyncio.sleep(self.MIN_INTERVAL - gap)
                RDService._last_call = time.monotonic()
                RDService._call_times.append(RDService._last_call)

            try:
                client_kwargs = (
                    {"transport": RDService._transport}
                    if RDService._transport is not None
                    else {"verify": False}
                )
                async with httpx.AsyncClient(**client_kwargs) as client:
                    resp = await client.request(method, url, **kwargs)
            except Exception as e:
                log_service.error(
                    f"RD: {method} {endpoint} transport error "
                    f"[{type(e).__name__}]: {e}"
                )
                return None

            if resp.status_code == 429:
                if attempt < self.MAX_RETRIES:
                    ra = (resp.headers.get("Retry-After") or "").strip()
                    delay = float(ra) if ra.replace(".", "", 1).isdigit() else min(2 ** attempt, 20)
                    log_service.warning(
                        f"RD: 429 rate-limited on {endpoint}; backing off "
                        f"{delay:.1f}s (retry {attempt + 1}/{self.MAX_RETRIES})"
                    )
                    await asyncio.sleep(delay)
                    continue
                # Retries exhausted: DON'T brute-force (RD blocks that). Trip the
                # breaker so every subsequent call short-circuits for the cooldown.
                RDService._trip_breaker(f"repeated 429 on {endpoint}")
                return resp
            return resp
        return resp

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalise(text: str) -> str:
        """Lowercase and replace common separators with a single space."""
        return re.sub(r"[\s._\-]+", " ", text.lower()).strip()

    @classmethod
    def _quality_rank(cls, filename: str) -> float:
        """Return quality rank from filename (higher = better)."""
        f = filename.lower()
        
        # Detect CAMs / theatrical rips / screeners (see CAM_PATTERN)
        if CAM_PATTERN.search(f):
            return 0.5
            
        if any(ind in f for ind in ["4k", "2160p", "2160", "uhd", "ultra hd", "ultrahd", "ultra-hd"]):
            return 4.0
        if any(ind in f for ind in ["1440p", "1440"]):
            return 3.5
        if any(ind in f for ind in ["1080p", "1080", "fhd"]):
            return 3.0
        if any(ind in f for ind in ["720p", "720", "hd"]):
            return 2.0
        if any(ind in f for ind in ["480p", "480"]):
            return 1.0
        return 0.0

    _EXTRAS_PATTERN = re.compile(
        r"(?:^|[/._\-\s])("
        r"featurettes?|extras?|bonus|samples?|trailers?|commentary|"
        r"deleted[\s._\-]+scenes?|"
        r"behind[\s._\-]+the[\s._\-]+scenes?|"
        r"making[\s._\-]+of|"
        r"interviews?"
        r")(?:[/._\-\s]|$)",
        re.IGNORECASE,
    )

    @classmethod
    def _is_extras_path(cls, file_path: str) -> bool:
        """True if the file path is inside an extras/featurettes/sample folder."""
        return bool(cls._EXTRAS_PATTERN.search(file_path))

    @staticmethod
    def _preferred_rank(quality: str) -> float:
        q = quality.lower()
        if q in ["4k", "2160p", "2160", "uhd"]: return 4.0
        if q in ["1440p", "1440"]: return 3.5
        if q in ["1080p", "1080", "fhd"]: return 3.0
        if q in ["720p", "720", "hd"]: return 2.0
        if q in ["480p", "480"]: return 1.0
        if q in ["cam"]: return 0.5
        return 0.0

    # ------------------------------------------------------------------
    # RD API calls
    # ------------------------------------------------------------------

    async def get_torrents(self) -> List[Dict]:
        """Return all torrents in the user's RD library (cached for 5 min)."""
        now = time.time()
        cached = self._cache.get(self.api_key)
        if cached and now - cached[0] < self.CACHE_TTL:
            log_service.info(f"RD: using cached torrent list ({len(cached[1])} items)")
            return cached[1]

        torrents: List[Dict] = []
        try:
            page = 1
            while True:
                resp = await self._request(
                    "GET",
                    f"{self.BASE_URL}/torrents",
                    params={"limit": 100, "page": page},
                )
                if resp is None or resp.status_code != 200:
                    if resp is not None:
                        log_service.error(
                            f"RD: /torrents returned {resp.status_code}"
                        )
                    break
                page_data = resp.json()
                if not page_data:
                    break
                torrents.extend(page_data)
                if len(page_data) < 100:
                    break
                page += 1

            self._cache[self.api_key] = (now, torrents)
            log_service.info(f"RD: fetched {len(torrents)} torrents from library")
        except Exception as e:
            log_service.error(f"RD: failed to fetch torrent list [{type(e).__name__}]: {e}")

        return torrents

    async def get_torrent_info(self, torrent_id: str) -> Optional[Dict]:
        """Get full info (files + links) for a single torrent (cached 60 min)."""
        now = time.time()
        cached = self._info_cache.get(torrent_id)
        if cached and now - cached[0] < self.INFO_CACHE_TTL:
            return cached[1]

        try:
            resp = await self._request(
                "GET", f"{self.BASE_URL}/torrents/info/{torrent_id}"
            )
            if resp is not None and resp.status_code == 200:
                info = resp.json()
                self._info_cache[torrent_id] = (now, info)
                return info
            if resp is not None:
                log_service.error(
                    f"RD: /torrents/info/{torrent_id} returned {resp.status_code}"
                )
        except Exception as e:
            log_service.error(
                f"RD: failed to get torrent info {torrent_id} [{type(e).__name__}]: {e}"
            )
        return None

    async def unrestrict_link(self, link: str) -> Optional[str]:
        """Convert an RD hoster link to a direct CDN download URL."""
        try:
            resp = await self._request(
                "POST",
                f"{self.BASE_URL}/unrestrict/link",
                data={"link": link},
            )
            if resp is not None and resp.status_code == 200:
                return resp.json().get("download")
            if resp is not None:
                log_service.error(
                    f"RD: /unrestrict/link returned {resp.status_code}: {resp.text[:200]}"
                )
        except Exception as e:
            log_service.error(
                f"RD: failed to unrestrict link [{type(e).__name__}]: {e}"
            )
        return None

    async def add_magnet(self, infohash: str) -> Optional[str]:
        """Add a magnet (by infohash) to RD; return the new torrent id."""
        magnet = f"magnet:?xt=urn:btih:{infohash}"
        try:
            resp = await self._request(
                "POST",
                f"{self.BASE_URL}/torrents/addMagnet",
                data={"magnet": magnet},
            )
            if resp is not None and resp.status_code in (200, 201):
                return resp.json().get("id")
            if resp is not None:
                log_service.error(
                    f"RD: /torrents/addMagnet returned {resp.status_code}: {resp.text[:200]}"
                )
        except Exception as e:
            log_service.error(f"RD: failed to add magnet [{type(e).__name__}]: {e}")
        return None

    async def select_all_files(self, torrent_id: str) -> None:
        """Select all files on a torrent so RD generates download links."""
        try:
            await self._request(
                "POST",
                f"{self.BASE_URL}/torrents/selectFiles/{torrent_id}",
                data={"files": "all"},
            )
        except Exception as e:
            log_service.error(
                f"RD: failed to select files for {torrent_id} [{type(e).__name__}]: {e}"
            )

    async def delete_torrent(self, torrent_id: str) -> None:
        """Remove a torrent from the RD library (used to clean up failed adds)."""
        try:
            await self._request(
                "DELETE", f"{self.BASE_URL}/torrents/delete/{torrent_id}"
            )
        except Exception as e:
            log_service.error(
                f"RD: failed to delete torrent {torrent_id} [{type(e).__name__}]: {e}"
            )

    def _pick_link(
        self,
        info: Dict,
        season: Optional[int],
        episode: Optional[int],
        filename_hint: Optional[str],
    ) -> Optional[str]:
        """
        Choose the RD hoster link for the requested file. RD's ``links`` array
        corresponds to the torrent's *selected* files, in file order; pair them
        and pick by episode pattern, then filename hint, then (for single-file
        torrents / movies) the largest file. For an ambiguous TV season pack we
        return None rather than risk serving the wrong episode.
        """
        files = info.get("files", [])
        links = info.get("links", [])
        selected = [f for f in files if f.get("selected") == 1]
        pairs = list(zip(selected, links))
        if not pairs:
            return None

        # Prefer real content over extras/samples when possible.
        candidates = [
            (f, ln) for f, ln in pairs if not self._is_extras_path(f.get("path", ""))
        ] or pairs

        # 1. Episode pattern (TV: season packs and single episodes)
        if season is not None and episode is not None:
            ep_pat = self._episode_pattern(season, episode)
            for f, ln in candidates:
                if ep_pat.search(f.get("path", "")):
                    return ln

        # 2. Exact filename hint from the addon URL
        if filename_hint:
            hint = self._normalise(filename_hint)
            for f, ln in candidates:
                if self._normalise(f.get("path", "").split("/")[-1]) == hint:
                    return ln

        # 3. Single file → use it. Ambiguous TV pack → don't guess.
        if len(candidates) == 1:
            return candidates[0][1]
        if season is not None and episode is not None:
            return None

        # 4. Movie / unknown → largest selected file.
        return max(candidates, key=lambda p: p[0].get("bytes", 0))[1]

    async def resolve_infohash(
        self,
        infohash: str,
        season: Optional[int] = None,
        episode: Optional[int] = None,
        filename_hint: Optional[str] = None,
    ) -> Optional[str]:
        """
        Convert a torrent infohash into a STABLE real-debrid.com direct-download
        URL (range-capable, survives mid-stream reconnects). Reuses an existing
        library entry for the same hash when present, else adds the magnet,
        selects files, picks the file for the requested episode, and unrestricts.

        Cached torrents play instantly; uncached-but-well-seeded ones are given a
        short bounded poll window to finish (POLL_ATTEMPTS × POLL_INTERVAL) and
        then play. Returns None when the torrent is still not ready after the
        window (left downloading on the account for a retry), is dead, or no
        suitable file matches — the caller skips to the next candidate.
        """
        infohash = infohash.lower()

        # Reuse an existing library entry for this hash to avoid duplicates.
        torrent_id = None
        for t in await self.get_torrents():
            if (t.get("hash") or "").lower() == infohash:
                torrent_id = t.get("id")
                break

        added = False
        if torrent_id is None:
            torrent_id = await self.add_magnet(infohash)
            if not torrent_id:
                return None
            added = True

        info = await self.get_torrent_info(torrent_id)
        if info and info.get("status") == "waiting_files_selection":
            await self.select_all_files(torrent_id)
            self._info_cache.pop(torrent_id, None)  # bust stale cache
            info = await self.get_torrent_info(torrent_id)

        if not info:
            if added:
                await self.delete_torrent(torrent_id)
            return None

        # Wait (bounded) for RD to make the torrent playable. Cached torrents are
        # already "downloaded" (loop body never runs). Uncached-but-well-seeded
        # ones flip to "downloaded" within a couple of seconds; we poll a few
        # times to catch them. Genuinely-stuck torrents are KEPT downloading (not
        # deleted) so a retry plays them — only truly-dead statuses are removed.
        status = (info or {}).get("status")
        polls = 0
        while status != "downloaded" and polls < self.POLL_ATTEMPTS:
            if status in self.DEAD_STATUSES:
                log_service.info(
                    f"RD: infohash {infohash[:8]} status={status} "
                    f"(will never download) — removing."
                )
                if added:
                    await self.delete_torrent(torrent_id)
                return None
            if status == "waiting_files_selection":
                # Late file-selection (e.g. after magnet_conversion) — kick it.
                await self.select_all_files(torrent_id)
            else:
                await asyncio.sleep(self.POLL_INTERVAL)
            self._info_cache.pop(torrent_id, None)  # force a fresh status read
            info = await self.get_torrent_info(torrent_id) or info
            status = (info or {}).get("status")
            polls += 1

        if status != "downloaded":
            # Not ready after the window. Leave it downloading on the account
            # (do NOT delete) so a retry shortly finds it cached — "just slower".
            log_service.info(
                f"RD: infohash {infohash[:8]} not ready yet (status={status}) "
                f"after {self.POLL_ATTEMPTS} polls — kept downloading, skipping "
                f"this candidate for now."
            )
            return None

        link = self._pick_link(info, season, episode, filename_hint)
        if not link:
            return None
        return await self.unrestrict_link(link)

    # ------------------------------------------------------------------
    # Title matching helpers
    # ------------------------------------------------------------------

    def _title_matches(self, torrent_name: str, title_words: List[str]) -> bool:
        """Return True if all significant words from the title appear in the torrent name."""
        norm = self._normalise(torrent_name)
        return all(w in norm for w in title_words)

    def _season_in_name(self, torrent_name: str, season: int) -> bool:
        """True if the torrent name references the requested season — including
        multi-season range packs (S01-13, Seasons 1-13) and complete-series packs."""
        name = torrent_name.lower()
        if "complete" in name:
            return True
        # Season ranges: S01-13, S01-S13, Season(s) 1-13, 1 to 13
        for m in re.finditer(
            r"s(?:eason)?s?\s*0*(\d{1,2})\s*(?:-|–|to)+\s*s?(?:eason)?\s*0*(\d{1,2})",
            name,
        ):
            if int(m.group(1)) <= season <= int(m.group(2)):
                return True
        # Single season: S08, Season 8, Season08
        return bool(re.search(rf"s(?:eason)?\s*0*{season}(?!\d)", name))

    @staticmethod
    def _episode_pattern(season: int, episode: int) -> "re.Pattern[str]":
        """Match an episode reference in SxxExx, SxEx, or NxNN form (e.g. 8x06)."""
        return re.compile(
            rf"(?:s0*{season}[\s._\-]*e0*{episode}|(?<!\d){season}x0*{episode})(?!\d)",
            re.IGNORECASE,
        )

    # ------------------------------------------------------------------
    # Public lookup methods
    # ------------------------------------------------------------------

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
        """
        Search the user's RD library for a specific TV episode.

        Matching strategy:
        1. Torrent filename must contain all words of the show title.
        2. Torrent filename must reference the season (s02, season2, etc.).
        3. File list is searched for the SxxExx pattern.
        4. Among all matching files, prefer the one whose quality is closest
           to `preferred_quality` (scored by abs rank difference).

        Returns a direct CDN download URL or None.
        """
        ep_re = self._episode_pattern(season, episode)
        title_words = [w for w in self._normalise(show_title).split() if len(w) > 1]
        pref_rank = self._preferred_rank(preferred_quality)

        torrents = await self.get_torrents()
        if not torrents:
            return None

        # --- Pass 1: filter candidates by title + season ---
        candidates = [
            t for t in torrents
            if self._title_matches(t.get("filename", ""), title_words)
            and self._season_in_name(t.get("filename", ""), season)
        ]

        if not candidates:
            log_service.info(
                f"RD: no library matches for '{show_title}' S{season:02d}E{episode:02d}"
            )
            return None

        log_service.info(
            f"RD: {len(candidates)} candidate torrent(s) for "
            f"'{show_title}' S{season:02d}E{episode:02d} (preferred quality: {preferred_quality})"
        )

        matches: List[tuple[float, str, str]] = []

        for torrent in candidates:
            info = await self.get_torrent_info(torrent["id"])
            if not info:
                continue

            files = info.get("files", [])
            links = info.get("links", [])
            if not files or not links:
                continue

            # RD only gives links for "selected" files; map file indices to link positions
            selected_map: List[int] = [
                i for i, f in enumerate(files) if f.get("selected", 0) == 1
            ]

            for link_pos, file_idx in enumerate(selected_map):
                if link_pos >= len(links):
                    break

                file_path = files[file_idx].get("path", "").lower()

                if not ep_re.search(file_path):
                    continue

                if self._is_extras_path(file_path):
                    log_service.info(
                        f"RD: skipping episode extra {files[file_idx].get('path')} (featurette/sample/extras)"
                    )
                    continue

                if _has_denied_video_codec(file_path):
                    log_service.info(
                        f"RD: skipping episode {files[file_idx].get('path')} "
                        f"(denylisted video codec — unplayable)"
                    )
                    continue

                # Foreign-dub release groups (ColdFilm/Ultradox/… RU voiceovers)
                # ship UNTAGGED non-English audio the validator can't catch by
                # tag — skip them here so the fast path returns the English copy
                # (e.g. MeGusta) directly instead of a Russian dub.
                if prefer_english and FOREIGN_DUB_RELEASE.search(file_path):
                    log_service.info(
                        f"RD: skipping foreign-dub episode {files[file_idx].get('path')} "
                        f"(untagged non-English release group)"
                    )
                    continue

                q_rank = self._quality_rank(file_path)

                # Never auto-serve cam-tier rips (CAM/TS/TC/DCP/SCR/…). q_rank 0.5
                # is the unique cam bucket from _quality_rank.
                if block_cam and q_rank == 0.5:
                    log_service.info(
                        f"RD: skipping cam-tier episode {files[file_idx].get('path')} "
                        f"(block_cam on)"
                    )
                    continue

                # On an EXPLICIT quality request, hard-skip mismatches (the caller
                # asked for a specific quality). On `auto`, quality is only a
                # preference: keep mismatches as lower-scored fallbacks so a
                # 720p-only episode still plays instead of 404'ing.
                if strict_quality and pref_rank > 0 and q_rank > 0 and q_rank != pref_rank:
                    log_service.info(
                        f"RD: skipping episode {files[file_idx].get('path')} "
                        f"(q_rank={q_rank}) — explicit quality request (pref_rank={pref_rank})"
                    )
                    continue

                score = 10 - abs(q_rank - pref_rank) - deprioritise_penalty(file_path)

                log_service.info(
                    f"RD: episode match — {files[file_idx].get('path')} "
                    f"(q_rank={q_rank}, score={score})"
                )

                matches.append((score, links[link_pos], files[file_idx].get('path')))

        if matches:
            # Sort descending by score
            matches.sort(key=lambda x: x[0], reverse=True)

            # Quality floor (auto only): the RD library is a fast path, but it
            # must not short-circuit playback with a copy far below the preferred
            # quality when a better source may exist via Stremio. If the best
            # downloaded match is more than one tier under the preference, defer
            # to the caller's quality-first Stremio walk (which will resolve and
            # seed a better source into RD for next time). Strict/explicit
            # requests and near-preferred matches (within one tier) still serve
            # instantly.
            if not strict_quality and pref_rank > 0:
                best_rank = self._quality_rank(matches[0][2])
                if best_rank > 0 and (pref_rank - best_rank) > 1.0:
                    log_service.info(
                        f"RD: best downloaded match {matches[0][2]} "
                        f"(q_rank={best_rank}) is >1 tier below preferred "
                        f"(pref_rank={pref_rank}); deferring to Stremio for a "
                        f"higher-quality source."
                    )
                    return None

            # The failover index walks the (potentially long) torrentio candidate
            # list; the RD library usually has only 1-2 matches for an episode.
            # Clamp rather than return None, so a climbing failover counter never
            # abandons a file we DO have downloaded (which would 404 through to
            # torrentio). RD library is the canonical best source — always serve it.
            # A runaway failover index (climbs while playback keeps retrying)
            # must clamp to the BEST match (index 0, matches are score-sorted
            # descending) — never the worst. Clamping to len-1 previously served
            # the lowest-quality/foreign copy of an episode we had in RD.
            start = use_index if use_index < len(matches) else 0
            if start != use_index:
                log_service.info(
                    f"RD: use_index {use_index} >= {len(matches)} match(es); "
                    f"clamping to best available (index {start})"
                )
            # Try the chosen match first, then any others (e.g. a duplicate library
            # copy of the same pack) so one dead RD link (hoster_unavailable / 503)
            # doesn't sink an episode we actually have another copy of.
            order = [start] + [i for i in range(len(matches)) if i != start]
            for i in order:
                match_score, match_url, match_path = matches[i]
                log_service.info(f"RD: unrestricting episode match at index {i} (score={match_score}): {match_path}")
                url = await self.unrestrict_link(match_url)
                if url:
                    return url
                log_service.info(f"RD: unrestrict failed for index {i}, trying next match")
            return None

        log_service.info(
            f"RD: no episode file for S{season:02d}E{episode:02d} "
            f"in {len(candidates)} candidate torrent(s)"
        )
        return None

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
        """
        Search the user's RD library for a movie file.
        Filters by title words (+ optional year), then picks the file
        with the best quality score that is at least 100 MB.
        """
        title_words = [w for w in self._normalise(movie_title).split() if len(w) > 1]
        pref_rank = self._preferred_rank(preferred_quality)

        torrents = await self.get_torrents()
        if not torrents:
            return None

        candidates = [
            t for t in torrents
            if self._title_matches(t.get("filename", ""), title_words)
            and (not year or str(year) in self._normalise(t.get("filename", "")))
        ]

        if not candidates:
            log_service.info(f"RD: no library matches for movie '{movie_title}' ({year})")
            return None

        log_service.info(
            f"RD: {len(candidates)} candidate torrent(s) for movie '{movie_title}'"
        )

        matches: List[tuple[float, str, str]] = []

        for torrent in candidates:
            info = await self.get_torrent_info(torrent["id"])
            if not info:
                continue

            files = info.get("files", [])
            links = info.get("links", [])
            if not files or not links:
                continue

            selected_map = [
                i for i, f in enumerate(files) if f.get("selected", 0) == 1
            ]

            for link_pos, file_idx in enumerate(selected_map):
                if link_pos >= len(links):
                    break

                file_size = files[file_idx].get("bytes", 0)
                if file_size < 100 * 1024 * 1024:
                    continue

                file_path = files[file_idx].get("path", "").lower()

                if self._is_extras_path(file_path):
                    log_service.info(
                        f"RD: skipping movie extra {files[file_idx].get('path')} (featurette/sample/extras)"
                    )
                    continue

                if _has_denied_video_codec(file_path):
                    log_service.info(
                        f"RD: skipping movie {files[file_idx].get('path')} "
                        f"(denylisted video codec — unplayable)"
                    )
                    continue

                # Foreign-dub release groups with untagged non-English audio
                # (see find_episode_stream).
                if prefer_english and FOREIGN_DUB_RELEASE.search(file_path):
                    log_service.info(
                        f"RD: skipping foreign-dub movie {files[file_idx].get('path')} "
                        f"(untagged non-English release group)"
                    )
                    continue

                q_rank = self._quality_rank(file_path)

                # Never auto-serve cam-tier rips (see find_episode_stream).
                if block_cam and q_rank == 0.5:
                    log_service.info(
                        f"RD: skipping cam-tier movie {files[file_idx].get('path')} "
                        f"(block_cam on)"
                    )
                    continue

                # Strict on explicit quality, preference on auto (see find_episode_stream).
                if strict_quality and pref_rank > 0 and q_rank > 0 and q_rank != pref_rank:
                    log_service.info(
                        f"RD: skipping movie {files[file_idx].get('path')} "
                        f"(q_rank={q_rank}) — explicit quality request (pref_rank={pref_rank})"
                    )
                    continue

                score = 10 - abs(q_rank - pref_rank) - deprioritise_penalty(file_path)

                log_service.info(
                    f"RD: movie match — {files[file_idx].get('path')} "
                    f"(q_rank={q_rank}, score={score})"
                )

                matches.append((score, links[link_pos], files[file_idx].get('path')))

        if matches:
            # Sort descending by score
            matches.sort(key=lambda x: x[0], reverse=True)

            # Quality floor (auto only) — see find_episode_stream. Don't let the
            # RD fast path short-circuit with a copy >1 tier below the preferred
            # quality; defer to the Stremio quality-first walk instead.
            if not strict_quality and pref_rank > 0:
                best_rank = self._quality_rank(matches[0][2])
                if best_rank > 0 and (pref_rank - best_rank) > 1.0:
                    log_service.info(
                        f"RD: best downloaded match {matches[0][2]} "
                        f"(q_rank={best_rank}) is >1 tier below preferred "
                        f"(pref_rank={pref_rank}); deferring to Stremio for a "
                        f"higher-quality source."
                    )
                    return None

            # Clamp (see find_episode_stream) — a climbing failover index must not
            # abandon a movie we have downloaded in RD.
            # A runaway failover index (climbs while playback keeps retrying)
            # must clamp to the BEST match (index 0, matches are score-sorted
            # descending) — never the worst. Clamping to len-1 previously served
            # the lowest-quality/foreign copy of an episode we had in RD.
            start = use_index if use_index < len(matches) else 0
            if start != use_index:
                log_service.info(
                    f"RD: use_index {use_index} >= {len(matches)} match(es); "
                    f"clamping to best available (index {start})"
                )
            order = [start] + [i for i in range(len(matches)) if i != start]
            for i in order:
                match_score, match_url, match_path = matches[i]
                log_service.info(f"RD: unrestricting movie match at index {i} (score={match_score}): {match_path}")
                url = await self.unrestrict_link(match_url)
                if url:
                    return url
                log_service.info(f"RD: unrestrict failed for index {i}, trying next match")
            return None

        log_service.info(f"RD: no suitable movie file found for '{movie_title}'")
        return None
