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
    CACHE_TTL = 300  # 5 minutes

    # Class-level cache keyed by api_key: (timestamp, torrent_list)
    _cache: Dict[str, tuple] = {}

    QUALITY_RANK = {"4k": 4, "2160p": 4, "1080p": 3, "720p": 2, "480p": 1}

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
    def _quality_rank(cls, filename: str) -> int:
        """Return quality rank from filename (higher = better)."""
        f = filename.lower()
        for label, rank in cls.QUALITY_RANK.items():
            if label in f:
                return rank
        return 0

    @staticmethod
    def _preferred_rank(quality: str) -> int:
        return {"4k": 4, "1080p": 3, "720p": 2, "480p": 1}.get(quality.lower(), 3)

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
        """Get full info (files + links) for a single torrent."""
        try:
            async with httpx.AsyncClient(verify=False) as client:
                resp = await client.get(
                    f"{self.BASE_URL}/torrents/info/{torrent_id}",
                    headers=self.headers,
                    timeout=10.0,
                )
                if resp.status_code == 200:
                    return resp.json()
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

        best_url: Optional[str] = None
        best_score = -1

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

                q_rank = self._quality_rank(file_path)
                score = 10 - abs(q_rank - pref_rank)

                log_service.info(
                    f"RD: episode match — {files[file_idx].get('path')} "
                    f"(q_rank={q_rank}, score={score})"
                )

                if score > best_score:
                    best_score = score
                    best_url = links[link_pos]

        if best_url:
            log_service.info("RD: unrestricting best episode match")
            return await self.unrestrict_link(best_url)

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

        best_url: Optional[str] = None
        best_score = -1

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
                q_rank = self._quality_rank(file_path)
                score = 10 - abs(q_rank - pref_rank)

                log_service.info(
                    f"RD: movie match — {files[file_idx].get('path')} "
                    f"(q_rank={q_rank}, score={score})"
                )

                if score > best_score:
                    best_score = score
                    best_url = links[link_pos]

        if best_url:
            log_service.info("RD: unrestricting best movie match")
            return await self.unrestrict_link(best_url)

        log_service.info(f"RD: no suitable movie file found for '{movie_title}'")
        return None
