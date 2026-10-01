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

The probe is injected because it MUST be the same catalogue playback will use. Zilean
is the primary source and searches by title+season+episode; the Stremio manifest is
only a fallback and searches by imdb id. They disagree on how this show's releases
are named, so measuring the wrong one picks the wrong season: measured live, Zilean
offered 7 candidates for Lizzie Borden at season 4 and 3 at season 1, while Torrentio
showed the opposite. Probing Torrentio and then playing through Zilean chose season 1
and episode 3 resolved to a 404.
"""

from typing import Callable, Dict, List, Optional, Tuple
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

    def __init__(self, probe: Callable, source_name: str = "provider"):
        """
        probe: async (imdb_id, title, season, episode) -> list of stream dicts.
               Must query whatever catalogue playback will query, so the season it
               measures is the season that will actually have files.
        """
        self.probe = probe
        self.source_name = source_name

    async def resolve_series(
        self, title: str, first_season: int = 1, episode_count: int = 1
    ) -> Optional[Tuple[str, int]]:
        """
        Return (imdb_id, season_offset) for `title`, or None if nothing is provable.

        season_offset is ADDED to TMDB season numbers to get the provider's season.
        episode_count is the season's episode count, used to sample across it.
        """
        candidates = await self._imdb_candidates(title)
        if not candidates:
            log_service.warning(f"Anthology resolver: no IMDB series found for '{title}'")
            return None

        episodes = self._sample_episodes(episode_count)
        for imdb_id, imdb_name in candidates:
            measured = await self._best_season(imdb_id, title, first_season, episodes)
            if measured is not None:
                season, score, covered = measured
                offset = season - first_season
                log_service.info(
                    f"Anthology resolver: '{title}' -> {imdb_id} season {season} "
                    f"(offset {offset:+d}) on {score} strictly matching release(s) "
                    f"across episodes {episodes}"
                    + ("" if covered else " — WARNING: not every sampled episode has "
                                          "a match, some may not play")
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

    @staticmethod
    def _sample_episodes(episode_count: int) -> List[int]:
        """
        Up to three episodes spread across the season: first, middle, last.

        Episode 1 alone is not evidence. A newly released season is on every indexer
        as episode 1 under every naming convention, while later episodes only appear
        under the one the scene actually settled on. Measured live: Lizzie Borden
        episode 1 had matches under both season hints, episode 3 only under one, and
        episode-1-only measurement therefore chose the hint that 404s mid-season.
        """
        count = max(1, int(episode_count or 1))
        return sorted({1, (count + 1) // 2, count})

    async def _best_season(
        self, imdb_id: str, title: str, first_season: int, episodes: List[int]
    ) -> Optional[Tuple[int, int, bool]]:
        """
        (season, total_score, covered) for the best season hint, or None if no hint
        matched anything at all.

        `covered` means every sampled episode had at least one strict match. A hint
        that covers the season always beats one that does not, however high the
        latter's total — a season that plays throughout is worth more than one with a
        large pile of candidates for its first episode and nothing for its third.
        Among equals, highest total wins; ties keep the lowest season, so an ordinary
        show lands on offset 0.
        """
        # Ranked on plausibly-English candidates, with the language-blind counts kept
        # as a fallback so a non-English title still resolves rather than failing.
        best_playable = None  # (covered, total, season)
        best_any = None
        empty_run = 0

        for season in range(first_season, first_season + MAX_SEASONS):
            playable, anylang = [], []
            for episode in episodes:
                try:
                    streams = await self.probe(imdb_id, title, season, episode)
                except Exception as e:
                    log_service.warning(
                        f"Anthology resolver: {self.source_name} lookup failed for "
                        f"{imdb_id}:{season}:{episode}: {e}"
                    )
                    streams = []
                anylang.append(
                    StremioService.episode_match_count(streams, title, season, episode)
                )
                playable.append(
                    StremioService.episode_match_count(
                        streams, title, season, episode, min_language_rank=2
                    )
                )

            if not sum(anylang):
                empty_run += 1
                if empty_run >= MAX_EMPTY_SEASONS:
                    break  # past the end of the series
                continue
            empty_run = 0

            covered = all(s > 0 for s in playable)
            log_service.info(
                f"Anthology resolver: {self.source_name} {imdb_id} season {season} "
                f"-> English-ish {playable}, any-language {anylang} for episodes "
                f"{episodes} of '{title}'"
                f"{' (every sampled episode playable)' if covered else ''}"
            )

            if best_any is None or (all(s > 0 for s in anylang), sum(anylang)) > (
                best_any[0], best_any[1]
            ):
                best_any = (all(s > 0 for s in anylang), sum(anylang), season)

            total = sum(playable)
            if total and (
                best_playable is None
                or (covered, total) > (best_playable[0], best_playable[1])
            ):
                best_playable = (covered, total, season)
                if covered and total >= CONFIDENT_MATCHES * len(episodes):
                    break  # plenty of evidence, stop spending provider requests

        best = best_playable or best_any
        if best is None:
            return None
        covered, total, season = best
        return season, total, covered
