"""Stream resolution API routes"""

import httpx
import re
import time
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from ..api.auth import get_current_user
from ..database import get_db
from ..models.user import User
from ..services.failover_manager import FailoverManager
from ..services.library_service import LibraryService
from ..services.log_service import log_service
from ..services.rd_service import RDService
from ..services.settings_manager import SettingsManager
from ..services.stremio_service import StremioService
from ..services.tmdb_service import TMDBService

router = APIRouter(prefix="/api/stream", tags=["stream"])

# Cache for resolved URLs: {key: (timestamp, url)}
RESOLVE_CACHE = {}
RESOLVE_CACHE_TTL = 3600  # 60 minutes


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

    # Get manifest URLs (support both list and single legacy format)
    manifest_urls = await settings.get("stremio_manifest_urls")
    if not manifest_urls:
        # Fallback to legacy single URL setting
        single_url = await settings.get("stremio_manifest_url")
        if single_url:
            manifest_urls = [single_url]

    if not manifest_urls:
        raise HTTPException(
            status_code=500, detail="No Stremio manifest URLs configured"
        )

    # Ensure it's a list
    if isinstance(manifest_urls, str):
        manifest_urls = [manifest_urls]

    failover = FailoverManager(db)

    try:
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

        # --- RD Direct Library Lookup ---
        rd_api_key_val = await settings.get("rd_api_key")
        rd_direct_enabled = await settings.get("rd_direct_enabled", False)

        if rd_api_key_val and rd_direct_enabled:
            rd_target_quality = quality
            if not quality or quality == "auto":
                rd_target_quality = await settings.get("series_preferred_quality", "1080p")

            try:
                if not tmdb and api_key:
                    tmdb = TMDBService(api_key)

                media_title = None
                media_year = None

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
                            f"RD direct: failed to get TMDB title for {media_type}/{tmdb_id}: {e}"
                        )

                if media_title:
                    rd = RDService(rd_api_key_val)
                    rd_url = None

                    if media_type == "tv":
                        rd_url = await rd.find_episode_stream(
                            media_title, season, episode, rd_target_quality, use_index
                        )
                    else:
                        rd_url = await rd.find_movie_stream(
                            media_title, media_year, rd_target_quality, use_index
                        )

                    if rd_url:
                        log_service.stream(
                            f"RD direct: {state_key} quality={rd_target_quality} "
                            f"→ {rd_url[:100]}..."
                        )
                        RESOLVE_CACHE[cache_key] = (time.time(), rd_url)
                        return RedirectResponse(url=rd_url, status_code=302)
                    else:
                        log_service.info(
                            f"RD direct: no match for {state_key}, falling back to Stremio addons"
                        )
                else:
                    log_service.info(
                        f"RD direct: could not determine title for {media_type}/{tmdb_id}, skipping"
                    )
            except Exception as e:
                log_service.error(f"RD direct lookup failed for {state_key}: {e}")

        # Try each manifest URL until we get streams
        streams = []
        stremio = None
        
        for manifest_url in manifest_urls:
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
            
        # Re-initialize StremioService with the successful URL for select_stream logic
        # Note: We closed it in the loop, but select_stream is a static/utility method on the instance
        # actually select_stream is an async method on the instance, so we need an open instance?
        # Let's check StremioService implementation. 
        # But wait, select_stream doesn't use self.manifest_url or http client. 
        # It just filters the list of streams. 
        # However, to be safe, let's keep the last used instance or create a new one.
        stremio = StremioService(manifest_url) 

        # Stop Index Increment Loop
        if use_index >= len(streams) and len(streams) > 0:
            log_service.info(
                f"Index {use_index} out of range (max {len(streams)-1}). Resetting to 0."
            )
            use_index = 0
            # Update state to reset
            state.current_index = 0
            await failover.update_state(state)

        fallback_enabled = await settings.get("quality_fallback_enabled", True)
        fallback_order = await settings.get(
            "quality_fallback_order", ["1080p", "720p", "4k", "480p"]
        )

        target_quality = quality
        if not quality or quality == "auto":
            target_quality = await settings.get("series_preferred_quality", "1080p")

        stream_url = await stremio.select_stream(
            streams,
            target_quality,
            use_index,
            fallback_enabled,
            fallback_order,
            season=season,
            episode=episode,
        )

        if not stream_url:
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

        log_service.stream(
            f"Resolved {state_key} quality={quality} index={use_index} attempt={state.attempt_count} → {stream_url[:100]}..."
        )

        # Resolve redirect chain with in-request retry on season-pack episode mismatch.
        # Each retry selects the next stream index immediately rather than waiting for
        # Jellyfin to make another request.
        MAX_EPISODE_RETRIES = 5

        final_url = None
        retry_stream_url = stream_url
        retry_index = use_index

        async with httpx.AsyncClient(follow_redirects=True, verify=False) as client:
            for retry in range(MAX_EPISODE_RETRIES + 1):
                if retry > 0:
                    retry_index += 1
                    next_url = await stremio.select_stream(
                        streams,
                        target_quality,
                        retry_index,
                        fallback_enabled,
                        fallback_order,
                        season=season,
                        episode=episode,
                    )
                    if not next_url or next_url == retry_stream_url:
                        log_service.warning(
                            f"No new stream at index {retry_index} for {state_key}, stopping retries."
                        )
                        break
                    retry_stream_url = next_url
                    log_service.info(
                        f"Episode mismatch retry {retry}/{MAX_EPISODE_RETRIES}: "
                        f"trying stream index {retry_index} → {retry_stream_url[:80]}..."
                    )

                try:
                    skip_head = any(
                        d in retry_stream_url
                        for d in ("torrentio", "real-debrid", "elfhosted")
                    )
                    if skip_head:
                        log_service.info(
                            f"Skipping HEAD for known blocking domain: {retry_stream_url}"
                        )

                    resolved = retry_stream_url

                    if not skip_head:
                        try:
                            log_service.info("Attempting HEAD request...")
                            response = await client.head(retry_stream_url, timeout=8.0)
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
                            "GET", retry_stream_url, timeout=8.0
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

                    # Correct episode (or movie/unknown) — accept this URL
                    final_url = resolved
                    break

                except Exception as e:
                    log_service.error(
                        f"Failed to resolve redirects (attempt {retry + 1}/{MAX_EPISODE_RETRIES + 1}) "
                        f"[{type(e).__name__}]: {e}"
                    )
                    continue

        if final_url:
            RESOLVE_CACHE[cache_key] = (time.time(), final_url)
        else:
            # All retries exhausted — serve the last resolved stream URL as a fallback
            final_url = retry_stream_url
            log_service.warning(
                f"All stream retries exhausted for {state_key}. "
                f"Serving best available — episode may be incorrect."
            )
            state.current_index = retry_index + 1
            state.attempt_count += 1
            await failover.update_state(state)

        return RedirectResponse(url=final_url, status_code=302)

    except HTTPException:
        raise
    except Exception as e:
        log_service.error(f"Stream resolution error: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to resolve stream: {str(e)}"
        )
    finally:
        if tmdb:
            await tmdb.close()
        if stremio:
            await stremio.close()
