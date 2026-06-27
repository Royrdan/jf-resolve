"""Real-Debrid direct library integration"""

import re
import time
from typing import Dict, List, Optional

import httpx

from .log_service import log_service


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

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {"Authorization": f"Bearer {api_key}"}

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
        
        # Detect CAMs/Screeners
        if re.search(r'\b(cam|camrip|hdcam|hdts|telesync|hdtc|screener|dvdscr)\b', f):
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
            async with httpx.AsyncClient(verify=False) as client:
                page = 1
                while True:
                    resp = await client.get(
                        f"{self.BASE_URL}/torrents",
                        headers=self.headers,
                        params={"limit": 100, "page": page},
                        timeout=10.0,
                    )
                    if resp.status_code != 200:
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
            async with httpx.AsyncClient(verify=False) as client:
                resp = await client.get(
                    f"{self.BASE_URL}/torrents/info/{torrent_id}",
                    headers=self.headers,
                    timeout=10.0,
                )
                if resp.status_code == 200:
                    info = resp.json()
                    self._info_cache[torrent_id] = (now, info)
                    return info
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
            async with httpx.AsyncClient(verify=False) as client:
                resp = await client.post(
                    f"{self.BASE_URL}/unrestrict/link",
                    headers=self.headers,
                    data={"link": link},
                    timeout=10.0,
                )
                if resp.status_code == 200:
                    return resp.json().get("download")
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
            async with httpx.AsyncClient(verify=False) as client:
                resp = await client.post(
                    f"{self.BASE_URL}/torrents/addMagnet",
                    headers=self.headers,
                    data={"magnet": magnet},
                    timeout=10.0,
                )
                if resp.status_code in (200, 201):
                    return resp.json().get("id")
                log_service.error(
                    f"RD: /torrents/addMagnet returned {resp.status_code}: {resp.text[:200]}"
                )
        except Exception as e:
            log_service.error(f"RD: failed to add magnet [{type(e).__name__}]: {e}")
        return None

    async def select_all_files(self, torrent_id: str) -> None:
        """Select all files on a torrent so RD generates download links."""
        try:
            async with httpx.AsyncClient(verify=False) as client:
                await client.post(
                    f"{self.BASE_URL}/torrents/selectFiles/{torrent_id}",
                    headers=self.headers,
                    data={"files": "all"},
                    timeout=10.0,
                )
        except Exception as e:
            log_service.error(
                f"RD: failed to select files for {torrent_id} [{type(e).__name__}]: {e}"
            )

    async def delete_torrent(self, torrent_id: str) -> None:
        """Remove a torrent from the RD library (used to clean up failed adds)."""
        try:
            async with httpx.AsyncClient(verify=False) as client:
                await client.delete(
                    f"{self.BASE_URL}/torrents/delete/{torrent_id}",
                    headers=self.headers,
                    timeout=10.0,
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
            ep_pat = re.compile(
                rf"s{season:02d}[\s._\-]*e{episode:02d}", re.IGNORECASE
            )
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

        Returns None when the torrent is not RD-cached (would need a download)
        or no suitable file is found — the caller should then fall back to the
        addon's own resolve URL.
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

        # Only instantly-playable (cached) torrents are usable for streaming.
        if info.get("status") != "downloaded":
            log_service.info(
                f"RD: infohash {infohash[:8]} not cached "
                f"(status={info.get('status')}), falling back to addon URL"
            )
            if added:
                await self.delete_torrent(torrent_id)
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

    def _contains_season(self, torrent_name: str, season: int) -> bool:
        norm = self._normalise(torrent_name).replace(" ", "")
        return bool(re.search(rf"s{season:02d}", norm))

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
        ep_pattern = rf"s{season:02d}e{episode:02d}"
        title_words = [w for w in self._normalise(show_title).split() if len(w) > 1]
        pref_rank = self._preferred_rank(preferred_quality)

        torrents = await self.get_torrents()
        if not torrents:
            return None

        # --- Pass 1: filter candidates by title + season ---
        candidates = [
            t for t in torrents
            if self._title_matches(t.get("filename", ""), title_words)
            and self._contains_season(t.get("filename", ""), season)
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

                if not re.search(ep_pattern, file_path):
                    continue

                if self._is_extras_path(file_path):
                    log_service.info(
                        f"RD: skipping episode extra {files[file_idx].get('path')} (featurette/sample/extras)"
                    )
                    continue

                q_rank = self._quality_rank(file_path)

                # If requested quality is explicit and this file has a detectable quality, they must match
                if pref_rank > 0 and q_rank > 0 and q_rank != pref_rank:
                    log_service.info(
                        f"RD: skipping episode {files[file_idx].get('path')} "
                        f"(q_rank={q_rank}) as it does not match requested quality (pref_rank={pref_rank})"
                    )
                    continue

                score = 10 - abs(q_rank - pref_rank)

                log_service.info(
                    f"RD: episode match — {files[file_idx].get('path')} "
                    f"(q_rank={q_rank}, score={score})"
                )

                matches.append((score, links[link_pos], files[file_idx].get('path')))

        if matches:
            # Sort descending by score
            matches.sort(key=lambda x: x[0], reverse=True)
            
            if use_index < len(matches):
                match_score, match_url, match_path = matches[use_index]
                log_service.info(f"RD: unrestricting episode match at index {use_index} (score={match_score}): {match_path}")
                return await self.unrestrict_link(match_url)
            else:
                log_service.info(f"RD: use_index {use_index} out of bounds (found {len(matches)} matches)")
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

                q_rank = self._quality_rank(file_path)

                # If requested quality is explicit and this file has a detectable quality, they must match
                if pref_rank > 0 and q_rank > 0 and q_rank != pref_rank:
                    log_service.info(
                        f"RD: skipping movie {files[file_idx].get('path')} "
                        f"(q_rank={q_rank}) as it does not match requested quality (pref_rank={pref_rank})"
                    )
                    continue

                score = 10 - abs(q_rank - pref_rank)

                log_service.info(
                    f"RD: movie match — {files[file_idx].get('path')} "
                    f"(q_rank={q_rank}, score={score})"
                )

                matches.append((score, links[link_pos], files[file_idx].get('path')))

        if matches:
            # Sort descending by score
            matches.sort(key=lambda x: x[0], reverse=True)
            
            if use_index < len(matches):
                match_score, match_url, match_path = matches[use_index]
                log_service.info(f"RD: unrestricting movie match at index {use_index} (score={match_score}): {match_path}")
                return await self.unrestrict_link(match_url)
            else:
                log_service.info(f"RD: use_index {use_index} out of bounds (found {len(matches)} matches)")
                return None

        log_service.info(f"RD: no suitable movie file found for '{movie_title}'")
        return None
