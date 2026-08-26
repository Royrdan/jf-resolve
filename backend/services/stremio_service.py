import asyncio
import json
import re
import time
from typing import Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .log_service import log_service


# Shared cam / theatrical-rip / screener detector — kept in sync with
# rd_service.CAM_PATTERN. Matches camcorder rips, telesyncs, telecines,
# digital-cinema rips (DCP/DCPRiP), pre-retail, screeners, mic-dubs (MD)
# and Korean-subbed cams. Below-retail sources we never auto-serve.
CAM_PATTERN = re.compile(
    r'\b(cam|camrip|hdcam|hqcam|ts|hdts|telesync|tc|hdtc|telecine|'
    r'scr|screener|dvdscr|dcp|dcprip|pdvd|predvd|korsub|md)\b'
)

# Fake-quality (AI upscale) and non-video (archive) markers — kept in sync with
# rd_service.UPSCALE_PATTERN / ARCHIVE_EXT_PATTERN. Streams matching these are
# pushed to the BOTTOM of the candidate order (last resort), so a genuine 1080p
# is always tried before a fake-4K upscale or a .rar archive.
_UPSCALE_PATTERN = re.compile(r"\b(?:ai[\s._\-]*)?upscal(?:e|ed|ing)\b", re.IGNORECASE)
_ARCHIVE_EXT_PATTERN = re.compile(
    r"\.(?:rar|zip|7z|tar|gz|bz2|r\d{2,3}|z\d{2}|\d{3})$", re.IGNORECASE
)


def _is_deprioritised_stream(stream: Dict) -> bool:
    text = f"{stream.get('title', '')} {stream.get('name', '')}"
    return bool(_UPSCALE_PATTERN.search(text) or _ARCHIVE_EXT_PATTERN.search(text))


# ── Candidate sort signals (REORDER ONLY — never discard) ───────────────────
# These rank candidates WITHIN a single resolution bucket so the resolve walk
# probes the likely-best source first (fewer wasted TorBox unrestrict + ffprobe
# round-trips). The validator + denylists remain the ONLY things that drop a
# source; a wrong guess here just changes probe order, never what gets served.
# All tiers are plain constants — tune them here.

# Audio-language likelihood from the release name (fallback when the source
# provides no structured `languages` list). English markers are checked before
# foreign ones so a MULTi/Dual release that also says FRENCH still ranks English.
_ENGLISH_AUDIO_MARKER = re.compile(
    r"\b(multi|dual[\s._-]?audio|dualaudio|vostfr|vostang|eng|english|"
    r"en[\s._-]?subs?|subbed)\b",
    re.IGNORECASE,
)
_FOREIGN_AUDIO_MARKER = re.compile(
    r"\b(french|truefrench|vff|vfq|vfi|vof|german|deutsch|italian|ita|"
    r"spanish|espanol|castellano|latino|dublado|dubbed|dublat|pldub|lektor|"
    r"russian|rus|hindi|tamil|telugu|korean|polish|czech|hungarian|"
    r"swedish|danish|greek|turkish|dubbing|dublaj|multidub|rusdub|itadub)\b",
    re.IGNORECASE,
)
# Editions that look broken on ordinary players (3D side-by-side / top-bottom).
# Not discarded — just sent to the back of their quality bucket.
_BAD_EDITION_MARKER = re.compile(
    r"\b(3d|h?sbs|htab|full[\s._-]?sbs|half[\s._-]?(?:sbs|ou))\b", re.IGNORECASE
)
# Extra credit for an explicit embedded-subtitle hint in the name (weak signal;
# real subtitle handling stays post-probe in stream.py).
_SUBTITLE_HINT_MARKER = re.compile(
    r"\b(vostfr|subbed|multi[\s._-]?subs?|esubs?|msubs?)\b", re.IGNORECASE
)

# (regex, score) tiers — first/highest match wins; unmatched → the neutral
# default noted on each helper.
_CONTAINER_TIERS = [
    (re.compile(r"\bmkv\b", re.IGNORECASE), 3),
    (re.compile(r"\b(mp4|m4v)\b", re.IGNORECASE), 2),
    (re.compile(r"\b(webm|avi)\b", re.IGNORECASE), 1),
    (re.compile(r"\b(ts|m2ts|wmv|iso|mpg|vob)\b", re.IGNORECASE), 0),
]
_SOURCE_TIERS = [
    (re.compile(r"\b(remux|bdremux|bd25|bd50)\b", re.IGNORECASE), 5),
    (re.compile(r"\b(bluray|blu-ray|bdrip|bd)\b", re.IGNORECASE), 4),
    (re.compile(
        r"\b(web[\s._-]?dl|webdl|amzn|dsnp|disnp|nf|nick|pmtp|hmax|max|atvp|"
        r"hulu|okko|playweb)\b", re.IGNORECASE), 3),
    (re.compile(r"\b(webrip|web)\b", re.IGNORECASE), 2),
    (re.compile(r"\b(brrip|hdrip)\b", re.IGNORECASE), 1),
    (re.compile(r"\b(hdtv|pdtv|dsr|dvdrip|dvd)\b", re.IGNORECASE), 1),
]
_CODEC_TIERS = [
    (re.compile(r"\b(x265|h\.?265|hevc)\b", re.IGNORECASE), 3),
    (re.compile(r"\b(x264|h\.?264|avc)\b", re.IGNORECASE), 3),
    (re.compile(r"\b(mpeg-?2|vc-?1)\b", re.IGNORECASE), 1),
    (re.compile(r"\b(xvid|divx)\b", re.IGNORECASE), 0),
]
_AUDIO_TIERS = [
    (re.compile(r"\b(atmos|truehd|dts[\s._-]?hd|dts[\s._-]?x)\b", re.IGNORECASE), 4),
    (re.compile(r"\bdts\b", re.IGNORECASE), 3),
    (re.compile(r"\b(ddp|dd\+|eac3|e-ac-3)\b", re.IGNORECASE), 3),
    (re.compile(r"\b(dd|ac3|ac-3)\b", re.IGNORECASE), 2),
    (re.compile(r"\baac\b", re.IGNORECASE), 1),
    (re.compile(r"\b(mp3|opus|vorbis)\b", re.IGNORECASE), 0),
]


def _tier_score(text: str, tiers, default: int) -> int:
    """Highest-scoring tier whose pattern appears in text, else default."""
    best = None
    for pat, score in tiers:
        if pat.search(text) and (best is None or score > best):
            best = score
    return default if best is None else best


def _language_rank(stream: Dict) -> int:
    """2 = English audio present, 1 = unmarked (English original by default),
    0 = foreign-dub only. Trusts the source's structured `languages` list first
    (Zilean), then falls back to reading the release name."""
    langs = stream.get("languages") or []
    if langs:
        norm = {str(l).strip().lower()[:2] for l in langs}
        return 2 if "en" in norm else 0
    text = f"{stream.get('title', '')} {stream.get('name', '')}"
    if _ENGLISH_AUDIO_MARKER.search(text):
        return 2
    if _FOREIGN_AUDIO_MARKER.search(text):
        return 0
    return 1


class StremioService:
    """Stremio addon manifest integration"""

    # Rate limiting: minimum delay between requests (in seconds)
    _last_request_time = 0
    _request_delay = 0.5  # 500ms

    # Cache: {key: (timestamp, streams)}
    _cache = {}
    _cache_ttl = 1800  # 30 minutes

    def __init__(self, manifest_url: str):
        self.manifest_url = self.normalize_url(manifest_url)

        # Create requests session with retries
        self.session = requests.Session()

        # Setup retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy, pool_connections=10, pool_maxsize=20
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Set browser-like headers
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
            }
        )

    @staticmethod
    def normalize_url(url: str) -> str:
        """
        Normalize Stremio manifest URL:
        - Convert stremio:// to https://
        - Remove /manifest.json
        """
        if not url:
            return ""

        if url.startswith("stremio://"):
            url = url.replace("stremio://", "https://", 1)

        # Remove /manifest.json and slashes
        url = url.replace("/manifest.json", "").rstrip("/")

        return url

    async def _rate_limited_request(self):
        """
        Implement rate limiting to avoid getting blocked by stream provider
        """
        current_time = time.time()
        time_since_last = current_time - StremioService._last_request_time

        if time_since_last < self._request_delay:
            delay = self._request_delay - time_since_last
            log_service.info(f"Rate limiting: waiting {delay:.2f}s before request")
            await asyncio.sleep(delay)

        StremioService._last_request_time = time.time()

    def _log_response_error_details(self, response: requests.Response, identifier: str):
        """
        Log
        """
        log_service.error(f"Response details for {identifier}:")
        log_service.error(f"  Status: {response.status_code}")
        log_service.error(f"  Headers: {dict(response.headers)}")
        log_service.error(
            f"  Content-Type: {response.headers.get('content-type', 'unknown')}"
        )
        log_service.error(f"  Content-Length: {len(response.content)} bytes")

        # Log
        try:
            preview = response.content[:500].decode("utf-8", errors="replace")
            log_service.error(f"  Content preview: {preview}")
        except Exception:
            log_service.error(
                f"  Content preview: <binary data, first 100 bytes: {response.content[:100]}>"
            )

    def _parse_json_safe(
        self, response: requests.Response, identifier: str
    ) -> Optional[Dict]:
        """
        Parse JSON
        """
        try:
            # Parse from bytes directly
            data = json.loads(response.content)
            return data
        except json.JSONDecodeError as e:
            log_service.error(f"JSON decode error for {identifier}: {e}")
            self._log_response_error_details(response, identifier)
            return None
        except Exception as e:
            log_service.error(
                f"Unexpected error parsing response for {identifier}: {e}"
            )
            self._log_response_error_details(response, identifier)
            return None

    async def get_movie_streams(self, imdb_id: str) -> List[Dict]:
        """
        Get streams for a movie
        """
        # Check cache
        cache_key = f"movie:{imdb_id}"
        if cache_key in self._cache:
            ts, cached_streams = self._cache[cache_key]
            if time.time() - ts < self._cache_ttl:
                log_service.info(f"Using cached streams for {cache_key}")
                return cached_streams

        # Rate limiting
        await self._rate_limited_request()

        url = f"{self.manifest_url}/stream/movie/{imdb_id}.json"

        log_service.info(f"Fetching Stremio streams from: {url}")

        try:
            # Use asyncio.to_thread
            response = await asyncio.to_thread(self.session.get, url, timeout=30)

            if response.status_code != 200:
                log_service.error(
                    f"Stremio API returned {response.status_code} for movie {imdb_id}"
                )
                self._log_response_error_details(response, f"movie {imdb_id}")
                return []

            # Parse JSON
            data = self._parse_json_safe(response, f"movie {imdb_id}")
            if data is None:
                return []

            streams = data.get("streams", [])
            log_service.info(f"Received {len(streams)} streams for movie {imdb_id}")
            self._cache[cache_key] = (time.time(), streams)
            return streams

        except requests.RequestException as e:
            log_service.error(f"HTTP error for movie {imdb_id}: {e} - URL: {url}")
            return []
        except Exception as e:
            log_service.error(
                f"Unexpected error fetching streams for movie {imdb_id}: {e}"
            )
            return []

    async def get_episode_streams(
        self, imdb_id: str, season: int, episode: int
    ) -> List[Dict]:
        """
        Get streams for a TV episode
        GET {manifest_url}/stream/series/{imdb_id}:{season}:{episode}.json
        """
        # Check cache
        cache_key = f"series:{imdb_id}:{season}:{episode}"
        if cache_key in self._cache:
            ts, cached_streams = self._cache[cache_key]
            if time.time() - ts < self._cache_ttl:
                log_service.info(f"Using cached streams for {cache_key}")
                return cached_streams

        # Rate limiting
        await self._rate_limited_request()

        url = f"{self.manifest_url}/stream/series/{imdb_id}:{season}:{episode}.json"

        log_service.info(f"Fetching Stremio streams from: {url}")

        try:
            # Use asyncio.to_thread
            response = await asyncio.to_thread(self.session.get, url, timeout=30)

            if response.status_code != 200:
                log_service.error(
                    f"Stremio API returned {response.status_code} for series {imdb_id}:{season}:{episode}"
                )
                self._log_response_error_details(
                    response, f"series {imdb_id}:{season}:{episode}"
                )
                return []

            # Parse JSON
            data = self._parse_json_safe(
                response, f"series {imdb_id}:{season}:{episode}"
            )
            if data is None:
                return []

            streams = data.get("streams", [])
            log_service.info(
                f"Received {len(streams)} streams for episode {imdb_id}:{season}:{episode}"
            )
            self._cache[cache_key] = (time.time(), streams)
            return streams

        except requests.RequestException as e:
            log_service.error(
                f"HTTP error for series {imdb_id}:{season}:{episode}: {e} - URL: {url}"
            )
            return []
        except Exception as e:
            log_service.error(
                f"Unexpected error fetching streams for series {imdb_id}:{season}:{episode}: {e}"
            )
            return []

    @staticmethod
    def _normalise_text(text: str) -> str:
        """Lowercase and collapse separators (whitespace, dots, underscores, dashes) to single spaces."""
        return re.sub(r"[\s._\-]+", " ", text.lower()).strip()

    @staticmethod
    def _stream_text(stream: Dict) -> str:
        """Concatenate every field that may contain the release/file name."""
        parts = [
            stream.get("title", "") or "",
            stream.get("name", "") or "",
        ]
        bh = stream.get("behaviorHints") or {}
        if isinstance(bh, dict):
            parts.append(bh.get("filename", "") or "")
            parts.append(bh.get("bingeGroup", "") or "")
        return " ".join(parts)

    @classmethod
    def _title_matches(cls, text: str, title: str) -> bool:
        """All significant title words (>1 char) must appear in the normalised text."""
        norm_text = cls._normalise_text(text)
        words = [w for w in cls._normalise_text(title).split() if len(w) > 1]
        if not words:
            return True
        return all(w in norm_text for w in words)

    @staticmethod
    def _episode_marker_matches(text: str, season: int, episode: int) -> bool:
        """
        True if `text` contains a marker for the given season/episode in any common form:
        s01e01, S1E1, s.1.e.1, s 1 e 1, s_1_e_1, 1x01, 01x1, "season 1 episode 1", etc.
        Leading zeros are optional; separators between markers can be dots, spaces, dashes,
        or underscores. Trailing digits are rejected so e1 doesn't match e10.
        """
        s = int(season)
        e = int(episode)
        text_lower = text.lower()
        sep = r'[\s._\-]*'
        patterns = [
            rf'(?<![a-z0-9])s{sep}0*{s}{sep}e{sep}0*{e}(?!\d)',
            rf'(?<![a-z0-9])0*{s}\s*x\s*0*{e}(?!\d)',
            rf'season{sep}0*{s}[\s._\-]+episode{sep}0*{e}(?!\d)',
            rf'episode{sep}0*{e}[\s._\-]+season{sep}0*{s}(?!\d)',
        ]
        return any(re.search(p, text_lower) for p in patterns)

    @staticmethod
    def _has_specific_episode_marker(text: str) -> bool:
        """
        True if `text` pins a *specific* single episode (any SxxExx / NxNN form).
        Used to tell a season pack (no episode pinned) apart from a different
        single episode we must not serve in place of the requested one.
        """
        t = text.lower()
        return bool(
            re.search(r'(?<![a-z0-9])s\s*\d{1,2}\s*e\s*\d{1,3}(?!\d)', t)
            or re.search(r'(?<![a-z0-9])\d{1,2}\s*x\s*\d{1,3}(?!\d)', t)
        )

    @classmethod
    def _season_pack_matches(cls, text: str, season: int) -> bool:
        """
        True if `text` looks like a pack that COVERS the requested season without
        pinning a single episode — a whole-season pack, a multi-season range, or a
        complete-series pack. Such packs contain the requested episode, and the
        provider file-picker (`_pick_file_id`) extracts the right file from them.
        A stream naming a specific *other* episode is rejected so we never serve
        the wrong episode from a season fallback.
        """
        if cls._has_specific_episode_marker(text):
            return False
        s = int(season)
        raw = text.lower()
        norm = re.sub(r"[\s._\-]+", " ", raw).strip()
        # Whole-season token: "s09", "s 9", "season 9"
        if re.search(rf'(?<![a-z0-9])s ?0*{s}(?![0-9e])', norm):
            return True
        if re.search(rf'(?<![a-z0-9])season ?0*{s}(?![0-9])', norm):
            return True
        # Season range on the raw text (dashes intact): "s01-13", "s01-s13", "1-13"
        for m in re.finditer(r'(?<![a-z0-9])s?0*(\d{1,2})\s*-\s*s?0*(\d{1,2})(?![0-9])', raw):
            lo, hi = int(m.group(1)), int(m.group(2))
            if lo <= s <= hi:
                return True
        # Complete-series / all-seasons indicators
        if re.search(r'\b(complete|all seasons|full series)\b', norm):
            return True
        return False

    @staticmethod
    def _year_conflicts(text: str, expected_year: int, tolerance: int = 1) -> bool:
        """
        True if the text contains 4-digit year tokens AND none of them match
        `expected_year` within `tolerance`. If no year tokens are present,
        we can't conclude a conflict.
        """
        years = [int(m.group()) for m in re.finditer(r'(?<!\d)(?:19|20)\d{2}(?!\d)', text)]
        if not years:
            return False
        return not any(abs(y - expected_year) <= tolerance for y in years)

    @classmethod
    def filter_streams_by_metadata(
        cls,
        streams: List[Dict],
        title: Optional[str],
        year: Optional[int] = None,
        season: Optional[int] = None,
        episode: Optional[int] = None,
    ) -> List[Dict]:
        """
        Soft filter: drop streams whose title/filename plainly does not match the
        requested media. If filtering eliminates every stream we return the
        original list (with a warning) so resolution can still proceed.
        """
        if not streams or not title:
            return streams

        matched: List[Dict] = []
        rejected_samples: List[str] = []

        is_episode = season is not None and episode is not None

        for stream in streams:
            text = cls._stream_text(stream)
            if not cls._title_matches(text, title):
                if len(rejected_samples) < 3:
                    rejected_samples.append(text[:120].replace("\n", " "))
                continue
            if is_episode and not (
                cls._episode_marker_matches(text, season, episode)
                or cls._season_pack_matches(text, season)
            ):
                if len(rejected_samples) < 3:
                    rejected_samples.append(text[:120].replace("\n", " "))
                continue
            if (not is_episode) and year and cls._year_conflicts(text, year):
                if len(rejected_samples) < 3:
                    rejected_samples.append(text[:120].replace("\n", " "))
                continue
            matched.append(stream)

        rejected_count = len(streams) - len(matched)
        if rejected_count:
            log_service.info(
                f"Stream metadata filter: rejected {rejected_count}/{len(streams)} stream(s) "
                f"that did not match title='{title}'"
                + (f" S{season:02d}E{episode:02d}" if is_episode else "")
                + (f" year={year}" if (not is_episode) and year else "")
                + (f". Samples: {rejected_samples}" if rejected_samples else "")
            )

        if not matched:
            log_service.warning(
                f"Stream metadata filter: no streams matched title='{title}'"
                + (f" S{season:02d}E{episode:02d}" if is_episode else "")
                + ". Falling back to unfiltered list."
            )
            return streams

        return matched

    @staticmethod
    def detect_quality(stream: Dict) -> str:
        """
        Detect quality from stream title/name
        Priority based on C# reference: 4K/2160p > 1440p > 1080p > 720p > 480p
        """
        title = stream.get("title", "").lower()
        name = stream.get("name", "").lower()
        text = f"{title} {name}"

        # Detect CAMs/Screeners first so they don't get misclassified as high quality
        if CAM_PATTERN.search(text):
            return "cam"

        # 4K / 2160p
        if any(ind in text for ind in ["4k", "2160p", "2160"]):
            return "4k"

        # 1440p
        if any(ind in text for ind in ["1440p", "1440"]):
            return "1440p"

        # 1080p / FHD
        if any(ind in text for ind in ["1080p", "1080", "fhd"]):
            return "1080p"

        # 720p / HD
        if any(ind in text for ind in ["720p", "720", "hd"]):
            return "720p"

        # 480p
        if any(ind in text for ind in ["480p", "480"]):
            return "480p"

        return "unknown"

    @staticmethod
    def _is_episode_specific(stream: Dict, season: int, episode: int) -> bool:
        """Return True if the stream title/name contains the specific SxxExx pattern."""
        title = stream.get("title", "").lower()
        name = stream.get("name", "").lower()
        text = f"{title} {name}"
        pattern = rf's{season:02d}e{episode:02d}'
        return bool(re.search(pattern, text))

    @staticmethod
    def _sort_by_episode_specificity(
        streams: List[Dict], season: int, episode: int
    ) -> List[Dict]:
        """
        Sort streams so episode-specific files come before season packs.
        Episode-specific: title/name contains S02E06.
        Season pack: title/name has S02 but no episode marker, or no marker at all.
        """
        episode_specific = []
        season_packs = []
        for s in streams:
            title = (s.get("title", "") + " " + s.get("name", "")).lower()
            if re.search(rf's{season:02d}e{episode:02d}', title):
                episode_specific.append(s)
            else:
                season_packs.append(s)

        if episode_specific:
            log_service.info(
                f"Stream prioritisation: {len(episode_specific)} episode-specific, "
                f"{len(season_packs)} season-pack streams for S{season:02d}E{episode:02d}"
            )
        return episode_specific + season_packs

    def _candidate_rank_key(
        self,
        stream: Dict,
        season: Optional[int],
        episode: Optional[int],
        english_first: bool,
    ) -> tuple:
        """Composite within-bucket sort key (higher tuple = probed first).

        Precedence, most significant first:
          1. English audio likely       (only when english_first)
          2. Episode-specific > pack     (TV correctness)
          3. Not a 3D/SBS edition        (broken-looking on normal players)
          4. Container   mkv>mp4>...     (best embedded subs / multi-audio)
          5. Source tier remux>web-dl>.. (better encode at same resolution)
          6. Codec       h265/h264>xvid
          7. Audio       atmos>ddp>ac3>aac
          8. Subtitle hint in the name   (weak; real subs handled post-probe)

        REORDER ONLY — nothing here drops a candidate.
        """
        text = f"{stream.get('title', '')} {stream.get('name', '')}"
        low = text.lower()
        lang = _language_rank(stream) if english_first else 1
        ep_specific = (
            1
            if season is not None
            and episode is not None
            and re.search(rf"s{season:02d}e{episode:02d}", low)
            else 0
        )
        edition_ok = 0 if _BAD_EDITION_MARKER.search(text) else 1
        return (
            lang,
            ep_specific,
            edition_ok,
            _tier_score(text, _CONTAINER_TIERS, 2),
            _tier_score(text, _SOURCE_TIERS, 2),
            _tier_score(text, _CODEC_TIERS, 2),
            _tier_score(text, _AUDIO_TIERS, 2),
            1 if _SUBTITLE_HINT_MARKER.search(text) else 0,
        )

    def ordered_candidates(
        self,
        streams: List[Dict],
        quality: str,
        fallback_enabled: bool = True,
        fallback_order: List[str] = None,
        season: Optional[int] = None,
        episode: Optional[int] = None,
        english_first: bool = True,
    ) -> List[str]:
        """
        Build a single flat, de-duplicated list of candidate stream URLs in
        resolution-preference order:

            requested quality (episode-specific first) →
            each fallback quality in order (episode-specific first) →
            any remaining streams (unknown quality bucket)

        The resolver's validation-retry loop walks THIS list, so when every
        stream in the requested quality fails (e.g. a dead Real-Debrid link),
        retries fall through to other qualities instead of dead-ending on a
        single-stream quality bucket. When fallback is disabled, only the
        requested quality's streams are returned.
        """
        if not streams:
            return []

        if fallback_order is None:
            fallback_order = ["1080p", "720p", "4k", "480p"]

        if fallback_enabled:
            ordered_qualities = [quality] + [
                q for q in fallback_order if q != quality
            ]
        else:
            ordered_qualities = [quality]

        seen = set()
        urls: List[str] = []

        # Genuine streams first, in quality order; fake-4K upscales and .rar
        # archives are held back and appended only at the very end (last resort).
        primary = [s for s in streams if not _is_deprioritised_stream(s)]
        deprioritised = [s for s in streams if _is_deprioritised_stream(s)]

        # Stable composite sort within each resolution bucket: English-audio
        # first, then episode-specificity, then file-quality tiers (container /
        # source / codec / audio) and edition sanity. Python's sort is stable,
        # so equal-key candidates keep the indexer's original order.
        def _bucket_sorted(items: List[Dict]) -> List[Dict]:
            return sorted(
                items,
                key=lambda s: self._candidate_rank_key(
                    s, season, episode, english_first
                ),
                reverse=True,
            )

        for q in ordered_qualities:
            q_streams = _bucket_sorted(
                [s for s in primary if self.detect_quality(s) == q]
            )
            for s in q_streams:
                url = s.get("url")
                if url and url not in seen:
                    seen.add(url)
                    urls.append(url)

        # Catch-all: include any streams whose detected quality wasn't in the
        # ordered list (only when fallback is enabled — better a wrong-quality
        # playable stream than nothing). Genuine (rank-sorted) first, then
        # de-prioritised upscales/archives last.
        if fallback_enabled:
            for s in _bucket_sorted(primary) + deprioritised:
                url = s.get("url")
                if url and url not in seen:
                    seen.add(url)
                    urls.append(url)

        return urls

    async def select_stream(
        self,
        streams: List[Dict],
        quality: str,
        index: int,
        fallback_enabled: bool = True,
        fallback_order: List[str] = None,
        season: Optional[int] = None,
        episode: Optional[int] = None,
    ) -> Optional[str]:
        """
        Select stream by quality and index.
        When season/episode are provided, episode-specific streams are preferred
        over season-pack streams within each quality tier.
        """
        if not streams:
            return None

        if fallback_order is None:
            fallback_order = ["1080p", "720p", "4k", "480p"]

        # Try requested quality first
        quality_streams = [s for s in streams if self.detect_quality(s) == quality]

        # Prioritise episode-specific streams over season packs
        if season is not None and episode is not None and quality_streams:
            quality_streams = self._sort_by_episode_specificity(
                quality_streams, season, episode
            )

        if quality_streams:
            idx = index
            if idx >= len(quality_streams):
                idx = index % len(quality_streams)
                log_service.info(
                    f"Index {index} out of range for quality {quality} "
                    f"({len(quality_streams)} streams). Wrapping to index {idx}."
                )

            return quality_streams[idx].get("url")

        # Fallback to other qualities if enabled
        if fallback_enabled:
            log_service.info(
                f"Quality {quality} not found, trying fallback order: {fallback_order}"
            )
            for fallback_quality in fallback_order:
                if fallback_quality == quality:
                    continue

                fallback_streams = [
                    s for s in streams if self.detect_quality(s) == fallback_quality
                ]

                if season is not None and episode is not None and fallback_streams:
                    fallback_streams = self._sort_by_episode_specificity(
                        fallback_streams, season, episode
                    )

                if fallback_streams:
                    log_service.info(f"Selected fallback quality: {fallback_quality}")
                    idx = index % len(fallback_streams)
                    return fallback_streams[idx].get("url")

        # Last resort: return first available stream
        if streams:
            log_service.info("No quality match found, using first available stream")
            return streams[0].get("url")

        return None

    async def close(self):
        """Close HTTP session"""
        self.session.close()
