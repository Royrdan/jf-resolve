"""Recover an IMDB id (and season mapping) when TMDB has none.

Anthologies are the problem case. Netflix's *Monster* is one TMDB show per season
("Monster: The Lizzie Borden Story", "Monster: The Ed Gein Story", …) but a single
multi-season series on IMDB (tt13207736). TMDB therefore has an EMPTY
external_ids.imdb_id for every one of them, and its season numbers don't line up
with what the providers index: TMDB season 1 of the Lizzie Borden show is season 4
of the IMDB series. Before this, add_to_library simply failed with
"IMDB ID not found" and the title never arrived.

The season mapping is never guessed. Guessing wrong is the worst possible outcome
here — it would silently play a different show (ask for Lizzie Borden, get Dahmer).
It is measured instead: ask the provider for episode 1 under each candidate season
and COUNT how many returned releases strictly identify both this show and this
episode, using the very predicate that decides playback
(StremioService.episode_match_count). The season hint with the most strict matches
wins, so what we store is the number most likely to actually play.

Why counting, and not "the first season whose names mention the title": the provider
is fuzzy on the season axis and returns a superset. Season 1 of tt13207736 comes back
with Dahmer files, Lizzie Borden files, AND whole-anthology packs named
"...S01E01-S04E08" that mention every season at once. A names-mention-the-title test
matches that pack under every season and picks the first one tried. Measured on real
data this mapped Lizzie Borden to Dahmer's season — exactly the silent wrong-show
failure this is meant to prevent.

Ties go to the lowest season, so an ordinary show settles on offset 0. If nothing
matches anywhere, this returns None and the caller fails loudly rather than inventing
a mapping.
"""

from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

import httpx

from .log_service import log_service
from .stremio_service import StremioService

# IMDB's own autocomplete endpoint — the same one their search box uses.
IMDB_SUGGEST_URL = "https://v3.sg.media-imdb.com/suggestion/x/{query}.json?includeVideos=0"

# Bounds on the probe. Each season costs one provider request (rate limited), so
# keep both small; anthologies are short and the match is usually found early.
MAX_CANDIDATES = 2
MAX_SEASONS = 8
MAX_EMPTY_SEASONS = 3

# Strict matches at which a season hint is considered settled, so the probe can stop
# early instead of walking every remaining season.
CONFIDENT_MATCHES = 3


class AnthologyResolver:
    """Finds an IMDB series and the season offset for a TMDB show that has no id."""

    def __init__(self, stremio: StremioService):
        self.stremio = stremio

    async def resolve_series(
        self, title: str, first_season: int = 1
    ) -> Optional[Tuple[str, int]]:
        """
        Return (imdb_id, season_offset) for `title`, or None if nothing is provable.

        season_offset is ADDED to TMDB season numbers to get the provider's season,
        so Lizzie Borden (TMDB season 1 == IMDB season 4) yields an offset of 3.
        """
        candidates = await self._imdb_candidates(title)
        if not candidates:
            log_service.warning(f"Anthology resolver: no IMDB series found for '{title}'")
            return None

        for imdb_id, imdb_name in candidates:
            measured = await self._best_season(imdb_id, title, first_season)
            if measured is not None:
                season, score = measured
                offset = season - first_season
                log_service.info(
                    f"Anthology resolver: '{title}' -> {imdb_id} season {season} "
                    f"(offset {offset:+d}) on {score} strictly matching release(s)"
                )
                return imdb_id, offset

        # Nothing proved. Accept a candidate only when its IMDB title is the SAME
        # show under a different id — that is an ordinary missing-id case (common for
        # brand-new titles the provider has no files for yet) and offset 0 is correct
        # by definition. An anthology can never reach here: its TMDB title carries the
        # season's subtitle, so it cannot equal the IMDB series name.
        norm_title = StremioService._normalise_text(title)
        for imdb_id, imdb_name in candidates:
            if StremioService._normalise_text(imdb_name) == norm_title:
                log_service.warning(
                    f"Anthology resolver: '{title}' -> {imdb_id} on an exact title "
                    f"match, offset 0. No provider files yet, so the mapping is "
                    f"unverified."
                )
                return imdb_id, 0

        log_service.warning(
            f"Anthology resolver: could not prove a season mapping for '{title}' "
            f"(tried {[c[0] for c in candidates]})"
        )
        return None

    async def _imdb_candidates(self, title: str) -> List[Tuple[str, str]]:
        """Series-only IMDB matches for a title, best first."""
        url = IMDB_SUGGEST_URL.format(query=quote(title))
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                response = await client.get(
                    url, headers={"User-Agent": "jf-resolve"}
                )
                response.raise_for_status()
                payload = response.json()
        except Exception as e:
            log_service.error(f"Anthology resolver: IMDB lookup failed for '{title}': {e}")
            return []

        out: List[Tuple[str, str]] = []
        for entry in payload.get("d", []):
            imdb_id = entry.get("id") or ""
            if not imdb_id.startswith("tt"):
                continue  # people, companies
            if not self._is_series(entry):
                continue
            out.append((imdb_id, entry.get("l") or ""))
            if len(out) >= MAX_CANDIDATES:
                break
        return out

    @staticmethod
    def _is_series(entry: Dict) -> bool:
        """Keep TV series / mini-series; drop films, shorts, episodes."""
        kind = f"{entry.get('q') or ''} {entry.get('qid') or ''}".lower()
        if not kind.strip():
            return True  # unlabelled: let the provider probe decide
        return "series" in kind

    async def _best_season(
        self, imdb_id: str, title: str, first_season: int
    ) -> Optional[Tuple[int, int]]:
        """
        (season, score) for the season hint with the most strictly matching releases,
        or None if no hint produced a single one.

        Ascending with a strictly-greater comparison, so ties keep the lowest season
        and an ordinary show lands on offset 0.
        """
        best_season = None
        best_score = 0
        empty_run = 0

        for season in range(first_season, first_season + MAX_SEASONS):
            try:
                streams = await self.stremio.get_episode_streams(imdb_id, season, 1)
            except Exception as e:
                log_service.warning(
                    f"Anthology resolver: provider lookup failed for "
                    f"{imdb_id}:{season}:1: {e}"
                )
                continue

            if not streams:
                empty_run += 1
                if empty_run >= MAX_EMPTY_SEASONS:
                    break  # past the end of the series
                continue
            empty_run = 0

            score = StremioService.episode_match_count(streams, title, season, 1)
            log_service.info(
                f"Anthology resolver: {imdb_id} season {season} -> {score} strict "
                f"match(es) for '{title}'"
            )
            if score > best_score:
                best_season, best_score = season, score
                if score >= CONFIDENT_MATCHES:
                    break  # plenty of evidence, stop spending provider requests

        if best_season is None:
            return None
        return best_season, best_score
