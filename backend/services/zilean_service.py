"""Zilean DMM-catalogue integration.

Zilean (iPromKnight) is a self-hosted ingester of the DebridMediaManager
hashlists — a purpose-built replacement for the public Torrentio addon, which
429-blocks the home IP, and for KnightCrawler, whose bulk DMM crawl never once
completed. Zilean holds ~1.5M parsed torrents locally and answers infohash
searches in one round-trip with zero external rate limits.

This service is deliberately shaped like :class:`StremioService`: its two entry
points return a list of **Stremio-style stream dicts** so the rest of the
resolve pipeline in ``stream.py`` (metadata filter → synthetic ``torrent://``
ref → TorBox ``resolve_infohash``) is completely source-agnostic. Each result
carries only an infohash + release title — never a playable URL — so every hit
flows through our own single-IP, cached-only TorBox resolution exactly like a
torrent-mode scraper result.

Query contract (confirmed against the live instance)::

    GET {base}/dmm/filtered?Query=<title>&Season=<s>&Episode=<e>
      -> JSON array of {info_hash, raw_title, resolution, seasons[],
                        episodes[], size, imdb_id, year, ...}

Note we search by **title**, not IMDb id: the instance runs with
``Zilean__Imdb__EnableImportMatching=false`` (a large ingest speed-up), so
torrents carry no ``imdb_id`` and an ``ImdbId=`` query returns nothing. The
canonical title comes from TMDB in ``stream.py``; Zilean does its own
normalised title match, and ``stream.py`` re-filters the ``raw_title`` on
title + SxxExx + year afterwards, so a loose server-side match is fine.
"""

from typing import Dict, List, Optional
from urllib.parse import quote

import httpx

from .log_service import log_service


class ZileanService:
    """Search a self-hosted Zilean instance for torrent infohashes."""

    def __init__(self, base_url: str):
        # Accept a bare host:port or a full URL; strip any trailing slash so
        # path joins are predictable.
        base_url = (base_url or "").strip().rstrip("/")
        if base_url and not base_url.startswith(("http://", "https://")):
            base_url = f"http://{base_url}"
        self.base_url = base_url

    async def _filtered(
        self,
        title: str,
        season: Optional[int] = None,
        episode: Optional[int] = None,
    ) -> List[Dict]:
        """Call /dmm/filtered and normalise hits into Stremio-style dicts.

        Returns [] on any error (empty title, transport failure, non-200,
        malformed body) so the caller can cleanly fall back to Stremio addons.
        """
        if not self.base_url or not title:
            return []

        params = f"Query={quote(title)}"
        if season is not None:
            params += f"&Season={int(season)}"
        if episode is not None:
            params += f"&Episode={int(episode)}"
        url = f"{self.base_url}/dmm/filtered?{params}"

        log_service.info(f"Zilean: querying {url}")

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(url)
        except Exception as e:
            log_service.error(f"Zilean: request failed for '{title}': {e}")
            return []

        if resp.status_code != 200:
            log_service.error(
                f"Zilean: returned {resp.status_code} for '{title}' "
                f"(S{season}E{episode})"
            )
            return []

        try:
            rows = resp.json()
        except Exception as e:
            log_service.error(f"Zilean: bad JSON for '{title}': {e}")
            return []

        if not isinstance(rows, list):
            log_service.error(
                f"Zilean: unexpected body shape for '{title}': {type(rows).__name__}"
            )
            return []

        streams: List[Dict] = []
        for row in rows:
            info_hash = row.get("info_hash") or row.get("infoHash")
            raw_title = row.get("raw_title") or row.get("parsed_title") or ""
            if not info_hash or not raw_title:
                continue
            resolution = row.get("resolution") or ""
            # Stremio-shaped: `title` holds the full release name (drives
            # detect_quality, the metadata filter, and the TorBox filename
            # hint); `name` labels the source + resolution for logs/UI. No
            # `url` — an infohash-only ref is synthesised downstream and MUST
            # go through cached-only TorBox resolution.
            streams.append(
                {
                    "infoHash": str(info_hash).lower(),
                    "title": raw_title,
                    "name": f"Zilean {resolution}".strip(),
                    "fileIdx": None,
                    "behaviorHints": {"filename": raw_title},
                }
            )

        log_service.info(
            f"Zilean: {len(streams)} usable candidate(s) for '{title}'"
            + (f" S{season:02d}E{episode:02d}" if season is not None and episode is not None else "")
        )
        return streams

    async def get_movie_streams(
        self, title: str, year: Optional[int] = None
    ) -> List[Dict]:
        """Infohash candidates for a movie. `year` is not sent to Zilean (its
        parsed year is unreliable, often 0); stream.py disambiguates on the
        raw_title text instead."""
        return await self._filtered(title)

    async def get_episode_streams(
        self, title: str, season: int, episode: int
    ) -> List[Dict]:
        """Infohash candidates for a TV episode. Season/episode are pushed to
        Zilean so it pre-filters server-side, shrinking the payload."""
        return await self._filtered(title, season, episode)
