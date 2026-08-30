"""Stream resolution API routes"""

import asyncio
import httpx
import re
import time
from datetime import datetime
from typing import Optional
from urllib.parse import quote, unquote, urlparse

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from ..api.auth import get_current_user
from ..database import get_db
from ..models.user import User
from ..services.failover_manager import FailoverManager
from ..services.library_service import LibraryService
from ..services.log_service import log_service
from ..services.rd_service import (
    RDService,
    RD_BLOCKED_RELEASE_TAGS,
    rd_filename_blocked,
)
from ..services.torbox_service import TorBoxService
from ..services.settings_manager import SettingsManager
from ..services.stream_validator import (
    DEFAULT_AUDIO_DENYLIST,
    DEFAULT_CONTAINER_DENYLIST,
    DEFAULT_VIDEO_DENYLIST,
    StreamValidator,
    ValidationPolicy,
)
from ..services.stremio_service import StremioService, CAM_PATTERN
from ..services.tmdb_service import TMDBService
from ..services.zilean_service import ZileanService

router = APIRouter(prefix="/api/stream", tags=["stream"])

# Explicit foreign-DUB filename markers. These indicate the ORIGINAL audio was
# replaced with a foreign dub (unwatchable in English). Deliberately does NOT
# match *SUBBED* tags (e.g. PLSUBBED) — a subbed release keeps the original
# English audio with foreign subtitles, which is fine. Used as a ground-truth
# check on the RESOLVED filename, where ffprobe language tags are unreliable
# (many dubs ship untagged as 'und').
FOREIGN_DUB_PATTERN = re.compile(
    r'\b(dubbing|dubbed|dublado|dublaj|dublagem|pldub|dubpl|lektor|'
    r'multidub|mdub|rusdub|itadub|castellano|latino)\b'
)


def _filename_from_url(url: str) -> str:
    """Best-effort decoded filename from a resolved stream URL (lowercased)."""
    try:
        return unquote(urlparse(url).path).rsplit("/", 1)[-1].lower()
    except Exception:
        return ""

# Cache for resolved URLs: {key: (timestamp, url)}
RESOLVE_CACHE = {}
RESOLVE_CACHE_TTL = 3600  # 60 minutes

# In-flight coalescing. Jellyfin fires the SAME resolve request several times in
# a few seconds (observed: 7 dup requests for one episode in ~45s). Without this,
# each duplicate independently walks all RD candidates in parallel — the bursts
# stacked up and tripped RD's rate breaker mid-play, so the first press failed and
# only the second (RD cache now warm) played. A per-key asyncio lock makes the
# duplicates queue: the leader does the one RD walk and fills RESOLVE_CACHE; the
# followers wake, hit that cache, and return instantly instead of hammering RD.
# Locks are ref-counted so the registry doesn't grow unbounded.
_RESOLVE_LOCKS: dict = {}
_RESOLVE_LOCK_REFS: dict = {}


def _acquire_resolve_lock(key: str):
    """Return the per-key lock, bumping its refcount. Single event loop → the
    get/create + refcount bump run without an await, so no race."""
    lock = _RESOLVE_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _RESOLVE_LOCKS[key] = lock
    _RESOLVE_LOCK_REFS[key] = _RESOLVE_LOCK_REFS.get(key, 0) + 1
    return lock


def _drop_resolve_lock(key: str):
    """Decrement the refcount; drop the lock from the registry once nobody holds
    a reference to it."""
    n = _RESOLVE_LOCK_REFS.get(key, 1) - 1
    if n <= 0:
        _RESOLVE_LOCK_REFS.pop(key, None)
        _RESOLVE_LOCKS.pop(key, None)
    else:
        _RESOLVE_LOCK_REFS[key] = n

# Matches a torrentio /resolve/ URL → captures (infohash, filename). Used to
# convert the addon's volatile resolve URL into a stable RD direct link.
_TORRENTIO_RESOLVE_RE = re.compile(
    r"/resolve/[^/]+/[^/]+/([0-9a-fA-F]{40})/[^/]+/\d+/([^/?]+)"
)


def _parse_torrentio_url(url: str):
    """Return (infohash, filename) for a torrentio resolve URL, else (None, None)."""
    m = _TORRENTIO_RESOLVE_RE.search(url)
    if not m:
        return None, None
    return m.group(1).lower(), unquote(m.group(2))


# Video-quality ordering so the subtitle hunt never demotes resolution. Higher
# rank = better picture. Mirrors StremioService.detect_quality buckets.
_QUALITY_RANK = {
    "cam": 0, "unknown": 1, "480p": 2, "720p": 3,
    "1080p": 4, "1440p": 5, "4k": 6,
}

# Torrentio encodes live seeders as "👤 N" in the stream title. Zilean rows
# carry no seeder count at all — those come back as None (unknown), which the
# uncached-load gate treats as "allow" (TorBox's own no-seeds stall-detection
# kills a truly dead magnet fast), while an explicit 0 is dropped.
_SEEDER_RE = re.compile(r'👤\s*(\d+)')


def _stream_seeders(stream: dict):
    """Best-effort seeder count for a candidate source dict, or None if unknown."""
    if not stream:
        return None
    bh = stream.get("behaviorHints") or {}
    for src in (stream, bh):
        for key in ("seeders", "seeds"):
            v = src.get(key)
            if isinstance(v, bool):
                continue
            if isinstance(v, int):
                return v
            if isinstance(v, str) and v.isdigit():
                return int(v)
    m = _SEEDER_RE.search(f"{stream.get('title', '')} {stream.get('name', '')}")
    return int(m.group(1)) if m else None


def _synth_torrent_ref(infohash: str, file_idx, name: str) -> str:
    """Synthetic candidate ref for TORRENT-MODE scraper results.

    When a scraper is configured without a debrid key it returns infoHash +
    fileIdx instead of a playable URL. We wrap those into
    ``torrent://<infohash>/<fileIdx>?name=<url-encoded name>``. These refs are
    NEVER playable directly — they MUST go through our own single-IP,
    cached-only RD conversion. That is exactly what keeps our RD key off the
    scraper's servers and out of the multi-IP ban pattern.
    """
    idx = file_idx if file_idx is not None else ""
    return f"torrent://{infohash.lower()}/{idx}?name={quote(name or '')}"


def _parse_stream_ref(url: str):
    """Return (infohash, file_idx, name, is_torrent_mode) for a candidate ref.

    Handles both our synthetic ``torrent://`` refs (torrent-mode: single-IP RD
    conversion is mandatory, cached-only, no raw fallback) and legacy torrentio
    ``/resolve/`` URLs (debrid-mode: a playable raw URL exists as a fallback).
    Returns (None, None, None, False) for a plain playable URL with no
    extractable infohash.
    """
    if url.startswith("torrent://"):
        rest = url[len("torrent://"):]
        path, _, query = rest.partition("?")
        infohash, _, idx = path.partition("/")
        name = unquote(query[len("name="):]) if query.startswith("name=") else ""
        file_idx = int(idx) if idx.isdigit() else None
        return infohash.lower(), file_idx, name, True
    infohash, name = _parse_torrentio_url(url)
    return infohash, None, name, False


@router.get("/providers")
async def provider_health(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Lightweight provider health for player error screens. Pings the ACTIVE debrid
    provider (TorBox or Real-Debrid) and the Zilean indexer with short timeouts so it
    returns fast even when a provider is hanging. Player clients call this on a resolve
    failure/timeout to show WHY (e.g. "TorBox down (403)") instead of a bare "timeout".
    Auth: internal-IP whitelist (same as /resolve), so LAN players can call it tokenless.
    """
    settings = SettingsManager(db)
    await settings.load_cache()

    out = {
        "provider": None,
        "debrid": {"status": "unknown", "message": ""},
        "zilean": {"status": "unknown", "message": ""},
        "overall": "ok",
    }

    debrid_provider = await settings.get("debrid_provider", "torbox") or "torbox"
    out["provider"] = debrid_provider

    # Active debrid provider (the usual point of failure).
    try:
        if debrid_provider == "rd":
            token = await settings.get("rd_api_key")
            url = "https://api.real-debrid.com/rest/1.0/user"
        else:
            token = await settings.get("torbox_api_key")
            url = "https://api.torbox.app/v1/api/torrents/mylist?limit=1"
        if not token:
            out["debrid"] = {"status": "not_configured", "message": "No API key set"}
            out["overall"] = "degraded"
        else:
            async with httpx.AsyncClient() as client:
                r = await client.get(
                    url, headers={"Authorization": f"Bearer {token}"}, timeout=8.0
                )
            if r.status_code == 200:
                out["debrid"] = {"status": "ok", "message": "Connected"}
            else:
                out["debrid"] = {"status": "error", "message": f"HTTP {r.status_code}"}
                out["overall"] = "degraded"
    except Exception as e:
        out["debrid"] = {"status": "error", "message": type(e).__name__}
        out["overall"] = "degraded"

    # Zilean indexer (best-effort; a candidate source, not fatal on its own).
    try:
        zilean_url = await settings.get("zilean_url", "")
        if not zilean_url:
            out["zilean"] = {"status": "not_configured", "message": "URL not set"}
        else:
            base = zilean_url.strip().rstrip("/")
            if base and not base.startswith(("http://", "https://")):
                base = f"http://{base}"
            async with httpx.AsyncClient() as client:
                r = await client.get(f"{base}/healthchecks/ping", timeout=4.0)
            if r.status_code == 200:
                out["zilean"] = {"status": "ok", "message": "Connected"}
            else:
                out["zilean"] = {"status": "error", "message": f"HTTP {r.status_code}"}
                if out["overall"] == "ok":
                    out["overall"] = "degraded"
    except Exception as e:
        out["zilean"] = {"status": "error", "message": type(e).__name__}
        if out["overall"] == "ok":
            out["overall"] = "degraded"

    return out


@router.api_route("/resolve/{media_type}/{tmdb_id}", methods=["GET", "HEAD"])
async def resolve_stream(
    media_type: str,
    tmdb_id: int,
    quality: str = Query("1080p"),
    season: Optional[int] = Query(None),
    episode: Optional[int] = Query(None),
    index: int = Query(0),
    imdb_id: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Resolve stream URL with failover
    Returns 302 redirect to actual stream URL from Stremio manifest
    """
    log_service.info(
        f"Stream resolve request: {media_type}/{tmdb_id} quality={quality} "
        f"index={index} imdb_id={imdb_id} season={season} episode={episode}"
    )

    # Check cache for resolved URL
    cache_key = f"{media_type}:{tmdb_id}:{season}:{episode}:{quality}:{index}"
    if cache_key in RESOLVE_CACHE:
        ts, cached_url = RESOLVE_CACHE[cache_key]
        if time.time() - ts < RESOLVE_CACHE_TTL:
            log_service.info(f"Using cached resolved URL for {cache_key}")
            return RedirectResponse(url=cached_url, status_code=302)

    if media_type not in ["movie", "tv"]:
        raise HTTPException(status_code=400, detail="Invalid media type")

    if media_type == "tv" and (season is None or episode is None):
        raise HTTPException(
            status_code=400, detail="Season and episode required for TV shows"
        )

    settings = SettingsManager(db)
    await settings.load_cache()

    tmdb = None
    api_key = await settings.get("tmdb_api_key")

    # Zilean is the primary self-hosted catalogue source (replaces public
    # Torrentio, which 429-blocks us). Read here so the manifest-required check
    # below can stand down when Zilean is providing the candidates.
    zilean_enabled = await settings.get("zilean_enabled", False)
    zilean_url = await settings.get("zilean_url", "")

    # Get manifest URLs (support both list and single legacy format)
    manifest_urls = await settings.get("stremio_manifest_urls")
    if not manifest_urls:
        # Fallback to legacy single URL setting
        single_url = await settings.get("stremio_manifest_url")
        if single_url:
            manifest_urls = [single_url]

    # Ensure it's a list
    if isinstance(manifest_urls, str):
        manifest_urls = [manifest_urls]
    if not manifest_urls:
        manifest_urls = []

    # Stremio manifests are only mandatory when Zilean is NOT the source. With
    # Zilean enabled, running with zero Stremio addons is a valid (and intended)
    # configuration — Zilean is primary, Stremio is just an optional fallback.
    if not manifest_urls and not (zilean_enabled and zilean_url):
        raise HTTPException(
            status_code=500, detail="No Stremio manifest URLs configured"
        )

    failover = FailoverManager(db)

    # Coalesce duplicate concurrent resolves for this exact key (see registry
    # above). Acquired as the first thing in the try so the finally always
    # releases it; released only if we actually took it.
    resolve_lock = _acquire_resolve_lock(cache_key)
    lock_held = False
    stremio = None  # hoisted: the finally references it, and the coalesced
                    # cache-hit can return before its in-body assignment

    try:
        await resolve_lock.acquire()
        lock_held = True

        # Re-check the cache under the lock: while we waited, the leader for this
        # key may have just resolved and cached it — serve that and skip the walk.
        if cache_key in RESOLVE_CACHE:
            ts, cached_url = RESOLVE_CACHE[cache_key]
            if time.time() - ts < RESOLVE_CACHE_TTL:
                log_service.info(
                    f"Coalesced resolve for {cache_key} — serving leader's cached URL"
                )
                return RedirectResponse(url=cached_url, status_code=302)

        if media_type == "movie":
            state_key = f"movie:{tmdb_id}"
        else:
            state_key = f"tv:{tmdb_id}:{season}:{episode}"

        grace_seconds = await settings.get("failover_grace_seconds", 45)
        reset_seconds = await settings.get("failover_window_seconds", 120)

        state = await failover.get_state(state_key)

        should_increment, use_index = failover.should_failover(
            state, grace_seconds, reset_seconds
        )

        now = datetime.utcnow()
        if state.first_attempt is None:
            state.first_attempt = now
        state.last_attempt = now

        if should_increment:
            state.current_index = use_index
            state.attempt_count += 1
        else:
            use_index = state.current_index

        await failover.update_state(state)

        if not imdb_id:
            if not api_key:
                raise HTTPException(
                    status_code=500, detail="TMDB API key not configured"
                )
            tmdb = TMDBService(api_key)
            library = LibraryService(db, tmdb, settings)
            imdb_id = await library.get_or_fetch_imdb_id(tmdb_id, media_type)

        if not imdb_id:
            log_service.error(f"No IMDB ID found for {media_type}:{tmdb_id}")
            raise HTTPException(status_code=404, detail="IMDB ID not found")

        stremio = None

        # Fetch TMDB metadata once — used by both RD direct lookup and the
        # post-Stremio metadata filter to reject mistagged streams.
        media_title = None
        media_year = None
        if not tmdb and api_key:
            tmdb = TMDBService(api_key)
        if tmdb:
            try:
                if media_type == "tv":
                    details = await tmdb.get_tv_details(tmdb_id)
                    media_title = details.get("name") or details.get("original_name")
                else:
                    details = await tmdb.get_movie_details(tmdb_id)
                    media_title = details.get("title") or details.get("original_title")
                    release_date = details.get("release_date", "")
                    if release_date:
                        try:
                            media_year = int(release_date.split("-")[0])
                        except (ValueError, IndexError):
                            pass
            except Exception as e:
                log_service.error(
                    f"Failed to fetch TMDB metadata for {media_type}/{tmdb_id}: {e}"
                )

        # --- Debrid provider selection (TorBox primary, RD fallback) ---
        # One setting swaps the whole direct-resolve path between providers.
        # TorBoxService is API-compatible with RDService (same 3 methods), so
        # everything downstream is provider-agnostic. `debrid_service_cls` is the
        # class used at every instantiation site; `debrid_api_key_val` keeps the
        # historical var name the enable-gates below read.
        debrid_provider = (
            await settings.get("debrid_provider", "torbox") or "torbox"
        ).lower()
        if debrid_provider == "rd":
            debrid_service_cls = RDService
            debrid_api_key_val = await settings.get("rd_api_key")
        else:
            debrid_provider = "torbox"
            debrid_service_cls = TorBoxService
            debrid_api_key_val = await settings.get("torbox_api_key")

        # --- Direct Library Lookup ---
        rd_api_key_val = debrid_api_key_val
        rd_direct_enabled = await settings.get("rd_direct_enabled", False)

        # When on (default), cam/telesync/DCP-rip/screener sources are never
        # served — better to show "unavailable" than a camcorder of an
        # in-cinema film. Applies to both the RD library lookup and the
        # Stremio addon fallback below.
        block_cam = await settings.get("block_cam", True)

        # Build the ffprobe validator ONCE, up front, so the same playability +
        # language policy governs BOTH the RD library lookup and the Stremio
        # candidates below. When prefer_english_audio is on (default), a source
        # whose audio is an explicit foreign-only dub is rejected and the
        # resolver falls through to an English/original-audio source.
        prefer_english_audio = await settings.get("prefer_english_audio", True)
        preferred_audio_langs = await settings.get("preferred_audio_langs", ["eng"])
        # Subtitle preference (soft tiebreaker, not a hard gate): among sources
        # that already pass the audio-language check, prefer one that carries a
        # preferred-language subtitle track; only fall back to a subtitle-less
        # source when no subtitled one is available.
        prefer_subtitles = await settings.get("prefer_subtitles", True)
        # Soft preference (not a hard gate): prefer a source carrying at least one
        # audio track the internal player can decode (AC3/EAC3/AAC/...), so a
        # DTS/TrueHD-only file — which plays silently — is only served when no
        # decodable-audio source is available.
        prefer_decodable_audio = await settings.get("prefer_decodable_audio", True)
        preferred_subtitle_langs = await settings.get("preferred_subtitle_langs", ["eng"])
        _pref_sub_set = {
            "eng" if s.strip().lower() in ("en", "eng", "english") else s.strip().lower()
            for s in preferred_subtitle_langs
        }
        validation_enabled = await settings.get("stream_validation_enabled", True)
        validator = None
        if validation_enabled:
            if StreamValidator.available():
                policy = ValidationPolicy(
                    min_duration_seconds=await settings.get("stream_min_duration_seconds", 180),
                    video_denylist=await settings.get("stream_video_denylist", DEFAULT_VIDEO_DENYLIST),
                    audio_denylist=await settings.get("stream_audio_denylist", DEFAULT_AUDIO_DENYLIST),
                    container_denylist=await settings.get("stream_container_denylist", DEFAULT_CONTAINER_DENYLIST),
                    probe_timeout_seconds=await settings.get("stream_probe_timeout_seconds", 10),
                    preferred_audio_langs=preferred_audio_langs,
                    require_preferred_audio=prefer_english_audio,
                    block_dv_no_fallback=await settings.get(
                        "block_dolby_vision_no_fallback", True
                    ),
                )
                validator = StreamValidator(policy)
            else:
                log_service.warning(
                    "stream_validation_enabled but ffprobe is not installed — "
                    "serving streams unvalidated"
                )

        if rd_api_key_val and rd_direct_enabled:
            rd_target_quality = quality
            rd_strict_quality = bool(quality and quality.lower() != "auto")
            if not quality or quality == "auto":
                rd_target_quality = await settings.get("series_preferred_quality", "1080p")

            try:
                if media_title:
                    rd = debrid_service_cls(rd_api_key_val)
                    rd_url = None

                    if media_type == "tv":
                        rd_url = await rd.find_episode_stream(
                            media_title, season, episode, rd_target_quality,
                            use_index, strict_quality=rd_strict_quality,
                            block_cam=block_cam, prefer_english=prefer_english_audio,
                        )
                    else:
                        rd_url = await rd.find_movie_stream(
                            media_title, media_year, rd_target_quality,
                            use_index, strict_quality=rd_strict_quality,
                            block_cam=block_cam, prefer_english=prefer_english_audio,
                        )

                    # Probe the RD library hit through the shared validator so
                    # the language gate (and playability) applies here too — this
                    # path bypasses the Stremio validation loop below. A foreign
                    # dub is rejected → fall through to the Stremio addons.
                    if rd_url and validator is not None:
                        rd_probe = await validator.validate(rd_url)
                        if not rd_probe.ok:
                            log_service.info(
                                f"{debrid_provider} direct: match rejected ({rd_probe.reason}) "
                                f"for {state_key} — falling back to Stremio addons"
                            )
                            rd_url = None

                    if rd_url:
                        log_service.stream(
                            f"{debrid_provider} direct: {state_key} quality={rd_target_quality} "
                            f"→ {rd_url[:100]}..."
                        )
                        RESOLVE_CACHE[cache_key] = (time.time(), rd_url)
                        return RedirectResponse(url=rd_url, status_code=302)
                    else:
                        log_service.info(
                            f"{debrid_provider} direct: no match for {state_key}, falling back to Stremio addons"
                        )
                else:
                    log_service.info(
                        f"{debrid_provider} direct: could not determine title for {media_type}/{tmdb_id}, skipping"
                    )
            except Exception as e:
                log_service.error(f"RD direct lookup failed for {state_key}: {e}")

        streams = []
        stremio = None
        # Keep a concrete manifest_url for the StremioService re-init below
        # (ordered_candidates/detect_quality are network-free utility methods);
        # defaults to "" when running Zilean-only with no addons configured.
        manifest_url = manifest_urls[0] if manifest_urls else ""

        # --- Primary source: Zilean (self-hosted DMM catalogue) ---
        # Query by canonical TMDB title (Zilean has no imdb_id when import
        # matching is off). Results are infohash-only and get resolved through
        # the same cached-only TorBox path as any torrent-mode candidate.
        if zilean_enabled and zilean_url:
            try:
                zilean = ZileanService(zilean_url)
                if media_title:
                    if media_type == "movie":
                        streams = await zilean.get_movie_streams(media_title, media_year)
                    else:
                        streams = await zilean.get_episode_streams(
                            media_title, season, episode
                        )
                    if streams:
                        log_service.info(
                            f"Zilean: {len(streams)} candidate(s) for {state_key} "
                            f"('{media_title}')"
                        )
                    else:
                        log_service.info(
                            f"Zilean: no candidates for {state_key} — "
                            f"falling back to Stremio addons"
                        )
                else:
                    log_service.info(
                        f"Zilean: no title for {media_type}/{tmdb_id}, skipping"
                    )
            except Exception as e:
                log_service.error(
                    f"Zilean query failed for {state_key}: {e} — "
                    f"falling back to Stremio addons"
                )

        # --- Fallback source: Stremio addons (only when Zilean gave nothing) ---
        # Try each manifest URL until we get streams
        for manifest_url in (manifest_urls if not streams else []):
            try:
                log_service.info(f"Attempting to fetch streams from: {manifest_url}")
                stremio = StremioService(manifest_url)

                if media_type == "movie":
                    current_streams = await stremio.get_movie_streams(imdb_id)
                else:
                    current_streams = await stremio.get_episode_streams(imdb_id, season, episode)

                if current_streams:
                    streams = current_streams
                    log_service.info(f"Successfully found {len(streams)} streams from {manifest_url}")
                    break
                else:
                    log_service.warning(f"No streams found from {manifest_url}, trying next..." if len(manifest_urls) > 1 else f"No streams found from {manifest_url}")

            except Exception as e:
                log_service.error(f"Error fetching from {manifest_url}: {e}")
                continue
            finally:
                if stremio:
                    await stremio.close()

        if not streams:
            log_service.error(
                f"All Stremio addons returned zero streams for {state_key} (IMDb: {imdb_id})"
            )
            raise HTTPException(
                status_code=404, detail="No streams available from any configured addon"
            )

        # Drop streams whose title/filename clearly does not match the requested
        # media — guards against indexer cross-tagging (e.g. unrelated torrents
        # returned for a given IMDb id).
        if media_title:
            streams = StremioService.filter_streams_by_metadata(
                streams,
                media_title,
                year=media_year if media_type == "movie" else None,
                season=season if media_type == "tv" else None,
                episode=episode if media_type == "tv" else None,
            )

        # Normalise TORRENT-MODE scraper results into synthetic candidate refs
        # so the rest of the pipeline is source-shape-agnostic. When a scraper is
        # given no debrid key it returns infoHash + fileIdx and no playable
        # `url`; wrap those so ordered_candidates/selection can carry them and the
        # resolve loop routes them through our single-IP cached-only RD path.
        # Debrid-mode streams already have a playable `url` and are left as-is.
        for s in streams:
            if not s.get("url") and s.get("infoHash"):
                s["url"] = _synth_torrent_ref(
                    s["infoHash"], s.get("fileIdx"),
                    s.get("title") or s.get("name") or "",
                )

        # Drop cam-tier sources entirely (default on). Better a clean 404 than
        # streaming a camcorder rip of something still in cinemas.
        if block_cam:
            before = len(streams)
            streams = [
                s for s in streams if StremioService.detect_quality(s) != "cam"
            ]
            dropped = before - len(streams)
            if dropped:
                log_service.info(
                    f"block_cam: dropped {dropped} cam-tier stream(s) for {state_key}"
                )
            if not streams:
                log_service.info(
                    f"block_cam: only cam-tier sources available for {state_key} — refusing"
                )
                raise HTTPException(
                    status_code=404,
                    detail="Only cam-tier sources available (blocked by block_cam)",
                )

        # Re-initialize StremioService with the successful URL for select_stream logic
        # Note: We closed it in the loop, but select_stream is a static/utility method on the instance
        # actually select_stream is an async method on the instance, so we need an open instance?
        # Let's check StremioService implementation. 
        # But wait, select_stream doesn't use self.manifest_url or http client. 
        # It just filters the list of streams. 
        # However, to be safe, let's keep the last used instance or create a new one.
        stremio = StremioService(manifest_url) 

        fallback_enabled = await settings.get("quality_fallback_enabled", True)
        fallback_order = await settings.get(
            "quality_fallback_order", ["1080p", "720p", "4k", "480p"]
        )

        target_quality = quality
        if not quality or quality == "auto":
            target_quality = await settings.get("series_preferred_quality", "1080p")

        # Convert torrentio addon resolve URLs into stable real-debrid.com
        # direct links so players survive mid-stream reconnects (no more 0:00
        # freezes). Falls back to the raw addon URL when RD can't resolve it
        # (not cached, no file match). Reuses the RD api key already loaded.
        rd_resolve_enabled = await settings.get("rd_resolve_torrentio_enabled", True)
        rd_converter = None
        if rd_api_key_val and rd_resolve_enabled:
            rd_converter = debrid_service_cls(rd_api_key_val)

        # Safety budget for torrent-mode plays: how many candidates we'll actually
        # push through the debrid provider. The candidate loop can walk many
        # streams for quality/validation reasons, but only this many will ever be
        # probed — the real cap on provider volume per play. R_BLOCKED tags are
        # skipped BEFORE they count against this budget.
        #
        # Provider-aware: RD's cache probe is an expensive add+poll+delete dance
        # that trips its rate breaker, so it stays tightly capped (4). TorBox's
        # probe is a single cheap /checkcached READ (no add, our own single IP,
        # multi-IP lenient service) — a low cap there just means a cached copy
        # deeper in the list never gets served (the Severance S02E02 bug). So
        # TorBox walks far deeper by default. Combined with the cached-first
        # pre-scan below, the walk almost always serves on probe #1 anyway.
        if debrid_provider == "torbox":
            rd_max_probes = await settings.get("torbox_max_probes", 40)
        else:
            rd_max_probes = await settings.get("rd_max_probes", 4)
        # RD's May-2026 filename filter-gate is RD-only; TorBox has no such
        # filter, so it must NOT pre-skip those releases (it serves them fine).
        rd_blocked_tags = (
            await settings.get("rd_blocked_release_tags", RD_BLOCKED_RELEASE_TAGS)
            if debrid_provider == "rd"
            else []
        )
        rd_probes_used = 0

        # Build one flat, de-duplicated candidate list ordered as
        # requested-quality → fallback-qualities → rest. The validation-retry
        # loop below walks THIS list, so a dead link in a single-stream quality
        # bucket falls through to other qualities instead of dead-ending.
        candidates = stremio.ordered_candidates(
            streams,
            target_quality,
            fallback_enabled,
            fallback_order,
            season=season,
            episode=episode,
            english_first=prefer_english_audio,
        )

        # Map each candidate URL back to its source dict so the uncached-load
        # pass (below) can read the seeder count / quality for a given ref.
        url_to_stream = {s["url"]: s for s in streams if s.get("url")}

        if not candidates:
            log_service.error(
                f"Stream selection failed for {state_key}. Quality requested: {target_quality}, "
                f"Index: {use_index}, Total streams: {len(streams)}"
            )
            available_qualities = set(stremio.detect_quality(s) for s in streams)
            log_service.error(
                f"Available qualities in addon response: {available_qualities}"
            )
            raise HTTPException(
                status_code=404, detail="No suitable stream quality found"
            )

        # --- TorBox cached-first pre-scan -------------------------------------
        # Batch-check EVERY candidate infohash against TorBox's cache in one
        # cheap read, then stable-reorder so already-cached sources lead the
        # walk. This is what makes playback reliable: a cached copy sitting
        # deeper in the candidate list (e.g. Severance S02E02's H.265-NTb at
        # index 4) is surfaced to the front and served on the first probe,
        # instead of the walk dead-ending on a probe cap or on uncached hashes
        # ahead of it. Quality order is preserved WITHIN the cached group and
        # within the uncached group, so this only ever promotes a cached source
        # over an equal-or-worse uncached one — never demotes quality between
        # two cached sources. No-op for RD (no cheap batch cache-check).
        if debrid_provider == "torbox" and rd_converter is not None and len(candidates) > 1:
            try:
                cand_hashes = []
                for url in candidates:
                    ih, _fi, _nm, is_tm = _parse_stream_ref(url)
                    cand_hashes.append(ih.lower() if (ih and is_tm) else None)
                to_check = [h for h in cand_hashes if h]
                if to_check:
                    cached_set = await rd_converter.check_cached_batch(to_check)
                    if cached_set:
                        cached_c = [
                            u for u, h in zip(candidates, cand_hashes)
                            if h and h in cached_set
                        ]
                        rest_c = [
                            u for u, h in zip(candidates, cand_hashes)
                            if not (h and h in cached_set)
                        ]
                        candidates = cached_c + rest_c
                        log_service.info(
                            f"TorBox cache pre-scan for {state_key}: "
                            f"{len(cached_set)}/{len(to_check)} candidate(s) "
                            f"cached — leading walk with cached sources."
                        )
                    else:
                        log_service.info(
                            f"TorBox cache pre-scan for {state_key}: "
                            f"0/{len(to_check)} candidate(s) cached — "
                            f"walk proceeds in quality order (uncached-load may follow)."
                        )
            except Exception as e:
                # Pre-scan is a best-effort optimisation; never let it break a
                # play. Fall through to the normal per-candidate walk.
                log_service.warning(
                    f"TorBox cache pre-scan failed for {state_key} "
                    f"([{type(e).__name__}] {e}) — continuing unordered."
                )

        # Bound the failover start index to the candidate list.
        if use_index >= len(candidates):
            log_service.info(
                f"Index {use_index} out of range (max {len(candidates)-1}). Resetting to 0."
            )
            use_index = 0
            state.current_index = 0
            await failover.update_state(state)

        # The in-request validation walk ALWAYS starts at the best-quality
        # candidate (index 0) and scans the whole list, regardless of the
        # failover offset. should_failover() advances current_index on elapsed
        # time (not proven playback failure), so a long/seeked healthy playback
        # can drift the persisted index past the best sources; honouring it as a
        # hard start-offset permanently demoted quality (e.g. skipping a cached
        # 1080p HEVC and landing on 480p). Per-candidate validation still skips
        # genuinely dead links, and the held-quality guard prevents demotion.
        walk_start = 0
        stream_url = candidates[walk_start]

        log_service.stream(
            f"Resolved {state_key} quality={quality} walk_start={walk_start} "
            f"(failover_index={use_index}) attempt={state.attempt_count} → "
            f"{stream_url[:100]}..."
        )

        # The ffprobe validator (playability + language gate) was built once up
        # front and is reused here for the Stremio candidate loop.

        # Resolve redirect chain with in-request retry on season-pack episode
        # mismatch AND failed playability validation. Each retry selects the next
        # stream index immediately rather than waiting for Jellyfin to re-request.
        MAX_EPISODE_RETRIES = await settings.get("stream_max_retries", 8)

        final_url = None
        retry_stream_url = stream_url
        retry_index = walk_start
        # Set when a candidate is rejected specifically for being a cam-tier or
        # foreign-dub source, so the exhausted-retries fallback 404s instead of
        # serving one of those (better "unavailable" than a camcorder/dub).
        saw_unacceptable = False
        # Subtitle hunt state: hold the BEST-quality playable candidate that
        # lacked a usable embedded subtitle while we scan up to 3 candidates for
        # one that has subs. Quality is never demoted to get a subtitle.
        held_no_sub_url = None
        held_no_sub_rank = -1
        sub_scans = 0
        # Parallel hold for the audio preference: best-quality DTS/TrueHD-only
        # source seen, served only if no decodable-audio source turns up.
        held_no_audio_url = None
        held_no_audio_rank = -1
        audio_scans = 0
        async with httpx.AsyncClient(follow_redirects=True, verify=False) as client:
            for retry in range(MAX_EPISODE_RETRIES + 1):
                if retry > 0:
                    retry_index += 1
                    if retry_index >= len(candidates):
                        log_service.warning(
                            f"No more candidates at index {retry_index} for {state_key} "
                            f"({len(candidates)} total across all qualities), stopping retries."
                        )
                        break
                    retry_stream_url = candidates[retry_index]
                    log_service.info(
                        f"Validation/mismatch retry {retry}/{MAX_EPISODE_RETRIES}: "
                        f"trying candidate index {retry_index} (cross-quality) → "
                        f"{retry_stream_url[:80]}..."
                    )

                try:
                    # Convert a torrentio resolve URL into a stable RD direct
                    # link. On success the player streams from real-debrid.com
                    # directly (range-capable, reconnect-safe). On failure we
                    # keep the addon URL and proceed as before.
                    play_url = retry_stream_url
                    infohash, _file_idx, fname, is_torrent_mode = _parse_stream_ref(
                        retry_stream_url
                    )
                    if infohash and rd_converter is not None:
                        # Skip release tags RD's filter-gate will 451 anyway —
                        # BEFORE they eat into the probe budget. (torrent-mode
                        # only: a debrid-mode ref still has a playable addon URL.)
                        if is_torrent_mode and rd_filename_blocked(
                            fname, rd_blocked_tags
                        ):
                            log_service.info(
                                f"RD: skipping blocked release tag for "
                                f"{state_key} ({(fname or '')[:60]}) — RD would "
                                f"reject it (filter-gate)."
                            )
                            continue
                        # Hard cap on how many candidates ever touch RD per play.
                        # Each uncached probe costs several RD calls (addMagnet +
                        # info + select + status polls); walking a long candidate
                        # list blows the self-imposed rate cap and trips the
                        # breaker, which then locks RD out for the whole cooldown
                        # — so every play during that window fails. Once the
                        # budget is spent we STOP calling RD: torrent-mode has no
                        # playable raw URL so it ends the walk, while debrid-mode
                        # keeps walking but plays the torrentio addon URL as-is.
                        rd_budget_spent = rd_probes_used >= rd_max_probes
                        if is_torrent_mode and rd_budget_spent:
                            log_service.warning(
                                f"RD probe budget ({rd_max_probes}) spent for "
                                f"{state_key} — no cached source found, stopping "
                                f"(protects the account)."
                            )
                            break
                        if rd_budget_spent:
                            # debrid-mode: budget spent — leave RD alone and play
                            # the addon URL for the remaining candidates.
                            log_service.info(
                                f"RD probe budget ({rd_max_probes}) spent for "
                                f"{state_key} — using addon URL without RD "
                                f"conversion (protects the account)."
                            )
                        else:
                            rd_probes_used += 1
                            direct = await rd_converter.resolve_infohash(
                                infohash, season, episode, filename_hint=fname
                            )
                            if direct:
                                log_service.stream(
                                    f"{debrid_provider}-converted infohash {infohash[:8]} for "
                                    f"{state_key} → {direct[:80]}..."
                                )
                                play_url = direct
                            elif is_torrent_mode:
                                # Torrent-mode ref with no cached RD copy: there is
                                # no playable raw URL to fall back to (it's just a
                                # magnet). Cached-only policy — skip to the next
                                # candidate rather than serve/queue a download.
                                log_service.info(
                                    f"RD: infohash {infohash[:8]} not cached for "
                                    f"{state_key} — skipping (cached-only)."
                                )
                                continue
                            else:
                                log_service.info(
                                    f"RD conversion unavailable for infohash "
                                    f"{infohash[:8]}, using addon URL"
                                )
                    elif is_torrent_mode:
                        # Torrent-mode but RD converter disabled/unavailable: a
                        # magnet ref is unplayable, so this candidate is unusable.
                        log_service.warning(
                            f"Torrent-mode candidate but no RD converter for "
                            f"{state_key} — skipping."
                        )
                        continue

                    skip_head = any(
                        d in play_url
                        for d in ("torrentio", "real-debrid", "elfhosted")
                    )
                    if skip_head:
                        log_service.info(
                            f"Skipping HEAD for known blocking domain: {play_url}"
                        )

                    resolved = play_url

                    if not skip_head:
                        try:
                            log_service.info("Attempting HEAD request...")
                            response = await client.head(play_url, timeout=8.0)
                            log_service.info(
                                f"HEAD response: {response.status_code} {response.url}"
                            )
                            if response.status_code == 405:
                                raise Exception("Method Not Allowed (405)")
                            resolved = str(response.url)
                        except Exception as e:
                            log_service.info(
                                f"HEAD failed ({e}), switching to GET stream..."
                            )
                            skip_head = True

                    if skip_head:
                        async with client.stream(
                            "GET", play_url, timeout=8.0
                        ) as response:
                            resolved = str(response.url)
                            log_service.info(f"GET response URL: {resolved}")

                    log_service.stream(f"Final resolved URL: {resolved[:100]}...")

                    # Check for season-pack episode mismatch
                    if media_type == "tv" and season is not None and episode is not None:
                        ep_match = re.search(rf's{season:02d}e(\d+)', resolved.lower())
                        if ep_match:
                            resolved_ep = int(ep_match.group(1))
                            if resolved_ep != episode:
                                log_service.warning(
                                    f"Season-pack mismatch (attempt {retry + 1}/{MAX_EPISODE_RETRIES + 1}) "
                                    f"for {state_key}: wanted E{episode:02d}, got E{resolved_ep:02d}."
                                    + (
                                        " Retrying next stream."
                                        if retry < MAX_EPISODE_RETRIES
                                        else " Retries exhausted."
                                    )
                                )
                                continue

                    # Ground-truth filename gate. The torrentio title was
                    # already cam-filtered, but rd_converter may resolve the
                    # infohash to a DIFFERENT file inside the torrent (e.g. a
                    # Polish DCP-rip dub). Re-check the ACTUAL resolved filename
                    # for cam-tier markers and foreign dubs — this is the only
                    # reliable signal for untagged dubs that ffprobe reports as
                    # 'und'.
                    resolved_name = _filename_from_url(resolved)
                    if resolved_name:
                        if block_cam and CAM_PATTERN.search(resolved_name):
                            saw_unacceptable = True
                            log_service.warning(
                                f"Resolved file is cam-tier "
                                f"({resolved_name[:80]}) for {state_key} "
                                f"(attempt {retry + 1}/{MAX_EPISODE_RETRIES + 1})"
                                + (" — trying next stream." if retry < MAX_EPISODE_RETRIES else " — retries exhausted.")
                            )
                            continue
                        if prefer_english_audio and FOREIGN_DUB_PATTERN.search(resolved_name):
                            saw_unacceptable = True
                            log_service.warning(
                                f"Resolved file is a foreign dub "
                                f"({resolved_name[:80]}) for {state_key} "
                                f"(attempt {retry + 1}/{MAX_EPISODE_RETRIES + 1})"
                                + (" — trying next stream." if retry < MAX_EPISODE_RETRIES else " — retries exhausted.")
                            )
                            continue

                    # Playability gate: probe the resolved file and reject dead
                    # links, non-media, too-short, or unplayable-codec streams.
                    if validator is not None:
                        probe = await validator.validate(resolved)
                        if not probe.ok:
                            log_service.warning(
                                f"Validation rejected stream (attempt {retry + 1}/"
                                f"{MAX_EPISODE_RETRIES + 1}) for {state_key}: "
                                f"{probe.reason}"
                                + (
                                    " — trying next stream."
                                    if retry < MAX_EPISODE_RETRIES
                                    else " — retries exhausted."
                                )
                            )
                            continue
                        log_service.stream(
                            f"Validation passed for {state_key}: "
                            f"fmt={probe.format_name} v={probe.video_codec} "
                            f"a={probe.audio_codec} langs={probe.audio_langs} "
                            f"default_audio={probe.default_audio_lang} "
                            f"subs={probe.sub_langs} dur={probe.duration}"
                        )

                        # Subtitle preference (soft): prefer a source carrying a
                        # usable subtitle track, but scan at most 3 playable
                        # candidates and NEVER demote video quality to get one.
                        # Any track counts as usable — untagged ('')/'und' tracks
                        # are almost always English on English releases. Bounding
                        # the hunt at 3 keeps our own (single-IP) RD unrestrict
                        # volume low while still finding embedded subs when close.
                        if prefer_subtitles and _pref_sub_set:
                            has_pref_sub = any(
                                (lang in _pref_sub_set) or lang in ("", "und", None)
                                for lang in probe.sub_langs
                            )
                            if not has_pref_sub:
                                cand_rank = _QUALITY_RANK.get(
                                    stremio.detect_quality(
                                        {"title": resolved_name or resolved}
                                    ),
                                    1,
                                )
                                # Hold the best-quality sub-less source and keep
                                # scanning; a lower-res subbed source can never
                                # displace it.
                                if held_no_sub_url is None or cand_rank > held_no_sub_rank:
                                    held_no_sub_url = resolved
                                    held_no_sub_rank = cand_rank
                                sub_scans += 1
                                if sub_scans >= 3:
                                    log_service.info(
                                        f"Subtitle hunt capped at 3 scans for "
                                        f"{state_key}; serving best-quality "
                                        f"playable source (external subs)."
                                    )
                                    final_url = held_no_sub_url
                                    break
                                # Keep looking for a subtitled candidate.
                                continue

                        # Audio preference (soft): avoid DTS/TrueHD-only sources
                        # the internal player can't decode (they play silently).
                        # Hold the best-quality such source and keep scanning
                        # (bounded at 3) for one with a decodable audio track.
                        if prefer_decodable_audio and not probe.has_decodable_audio:
                            cand_rank = _QUALITY_RANK.get(
                                stremio.detect_quality(
                                    {"title": resolved_name or resolved}
                                ),
                                1,
                            )
                            if held_no_audio_url is None or cand_rank > held_no_audio_rank:
                                held_no_audio_url = resolved
                                held_no_audio_rank = cand_rank
                            audio_scans += 1
                            if audio_scans >= 3:
                                log_service.info(
                                    f"Audio hunt capped at 3 scans for {state_key}; "
                                    f"serving best-quality playable source "
                                    f"(DTS/TrueHD-only)."
                                )
                                final_url = held_no_audio_url
                                break
                            # Keep looking for a decodable-audio candidate.
                            continue

                    # Correct episode (or movie/unknown), playable, and either
                    # subtitle-satisfied or subtitles not required — accept this
                    # first (best-quality) acceptable source.
                    final_url = resolved
                    break

                except Exception as e:
                    log_service.error(
                        f"Failed to resolve redirects (attempt {retry + 1}/{MAX_EPISODE_RETRIES + 1}) "
                        f"[{type(e).__name__}]: {e}"
                    )
                    continue

        # Subtitle hunt ended without a subtitled candidate (ran out of
        # candidates before the 3-scan cap): serve the best-quality sub-less
        # source we held.
        if final_url is None and held_no_sub_url is not None:
            final_url = held_no_sub_url

        # Audio hunt ran out of candidates without a decodable-audio source:
        # serve the best-quality DTS/TrueHD-only source we held (audible only if
        # the player later switches tracks, but better than a not-cached stub).
        if final_url is None and held_no_audio_url is not None:
            final_url = held_no_audio_url

        # ── Pass 2: uncached load-and-wait (opt-in) ────────────────────────
        # Pass 1 walked every candidate cached-only and found nothing playable.
        # Before falling back to a 404 / not-cached stub, optionally queue the
        # best UNCACHED candidate onto TorBox, wait a bounded window for it to
        # download, and serve it on the fly. Any title with a cached source is
        # served in Pass 1 and never reaches here, so the hot path is untouched.
        uncached_attempted = False
        allow_uncached = await settings.get("allow_uncached_load", False)
        if (
            final_url is None
            and allow_uncached
            and debrid_provider == "torbox"
            and rd_converter is not None
        ):
            min_seeders = await settings.get("uncached_min_seeders", 1)
            wait_budget = await settings.get("uncached_load_wait_seconds", 20)

            # Rank the uncached candidates by (quality, seeders). Explicit
            # 0-seed torrents can never finish downloading, so drop them;
            # unknown-seeder rows (Zilean) are allowed — TorBox's stall
            # detection abandons a genuinely dead magnet quickly.
            ranked = []
            seen_hashes = set()
            for cand_url in candidates:
                ih, _fidx, cname, _tm = _parse_stream_ref(cand_url)
                if not ih or ih in seen_hashes:
                    continue
                seen_hashes.add(ih)
                src = url_to_stream.get(cand_url, {})
                seeders = _stream_seeders(src)
                if seeders is not None and seeders < min_seeders:
                    continue
                rank = _QUALITY_RANK.get(stremio.detect_quality(src), 1)
                ranked.append((rank, seeders or 0, ih, cname))
            ranked.sort(key=lambda x: (x[0], x[1]), reverse=True)

            if not ranked:
                log_service.info(
                    f"Uncached-load: no candidate with ≥{min_seeders} seeder(s) "
                    f"for {state_key} — nothing to queue."
                )
            else:
                uncached_attempted = True
                # Try the best few, but keep the TOTAL wait bounded (shared
                # deadline) so Jellyfin's play request doesn't time out when the
                # first magnet is slow.
                load_deadline = time.monotonic() + wait_budget
                for rank, seeders, ih, cname in ranked[:3]:
                    remaining = load_deadline - time.monotonic()
                    if remaining < 3:
                        break
                    log_service.info(
                        f"Uncached-load: queuing {ih[:8]} "
                        f"({(cname or '')[:50]}, 👤{seeders or '?'}) onto TorBox, "
                        f"waiting ≤{remaining:.0f}s for {state_key}."
                    )
                    loaded = await rd_converter.load_and_wait(
                        ih, season, episode,
                        filename_hint=cname,
                        wait_budget=remaining,
                    )
                    if not loaded:
                        continue
                    if validator is not None:
                        probe = await validator.validate(loaded)
                        if not probe.ok:
                            log_service.warning(
                                f"Uncached-load: {ih[:8]} loaded but failed "
                                f"validation ({probe.reason}) for {state_key}."
                            )
                            continue
                    log_service.stream(
                        f"Uncached-load: served freshly-loaded {ih[:8]} for "
                        f"{state_key} → {loaded[:80]}..."
                    )
                    final_url = loaded
                    break

        if final_url:
            RESOLVE_CACHE[cache_key] = (time.time(), final_url)
        elif uncached_attempted:
            # We queued an uncached download but it wasn't ready inside the wait
            # window (it keeps downloading server-side). Serving the not-cached
            # addon stub here would just play a ~30s "downloading" clip, so
            # return unavailable — the next play press should hit cache.
            log_service.warning(
                f"Uncached-load in progress for {state_key} — not ready yet. "
                f"Returning 404 (retry shortly to play from cache)."
            )
            raise HTTPException(
                status_code=404,
                detail="Source is caching now — try again in a moment",
            )
        elif saw_unacceptable:
            # Every remaining candidate was a cam-tier or foreign-dub source.
            # Refuse rather than serve one — Jellyfin shows "unavailable".
            log_service.warning(
                f"All acceptable candidates exhausted for {state_key} — only "
                f"cam-tier/foreign-dub sources remain. Returning 404."
            )
            raise HTTPException(
                status_code=404,
                detail="Only cam-tier or foreign-dub sources available (blocked)",
            )
        elif retry_stream_url and not retry_stream_url.startswith("torrent://"):
            # Debrid-mode only: serve the last resolved addon URL as a fallback.
            # (Torrent-mode refs are unplayable magnets, so they never land here.)
            final_url = retry_stream_url
            log_service.warning(
                f"All stream retries exhausted for {state_key}. "
                f"Serving best available — episode may be incorrect."
            )
            state.current_index = retry_index + 1
            state.attempt_count += 1
            await failover.update_state(state)
        else:
            # Torrent-mode: nothing cached and playable across all candidates.
            log_service.warning(
                f"No cached, playable source for {state_key} across "
                f"{retry_index + 1} candidate(s) — returning 404."
            )
            raise HTTPException(
                status_code=404, detail="No cached playable source available"
            )

        return RedirectResponse(url=final_url, status_code=302)

    except HTTPException:
        raise
    except Exception as e:
        log_service.error(f"Stream resolution error: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to resolve stream: {str(e)}"
        )
    finally:
        if lock_held:
            resolve_lock.release()
        _drop_resolve_lock(cache_key)
        if tmdb:
            await tmdb.close()
        if stremio:
            await stremio.close()
