"""Media-segment (intro/credits) API.

Non-blocking by design: a request returns the cached result immediately if we
have one, otherwise it kicks off detection in the background and returns
``{"status": "pending"}`` right away — so a player (Wholphin) can call it at
pre-warm time to trigger analysis of the next episode, then again at play time
to fetch the ready result. Detection itself lives in ``segment_service``.

Auth: same internal-IP whitelist as /resolve (``get_current_user`` treats
private/loopback callers as authorised), so LAN players call it tokenless.
"""

import asyncio
import json
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from ..api.auth import get_current_user
from ..database import AsyncSessionLocal, get_db
from ..models.media_segment import MediaSegment
from ..models.user import User
from ..services import segment_service
from ..services.log_service import log_service

router = APIRouter(prefix="/api/segments", tags=["segments"])

# Keys currently being detected, so concurrent requests don't fan out duplicate
# fingerprint jobs for the same episode.
_inflight: set = set()


def _key(media_type: str, tmdb_id: int, season: Optional[int], episode: Optional[int]) -> str:
    return f"{media_type}:{tmdb_id}:{season}:{episode}"


async def _detect_and_store(key: str, media_type: str, tmdb_id: int,
                            imdb_id: Optional[str], season: Optional[int],
                            episode: Optional[int]) -> None:
    try:
        res = await segment_service.detect(
            media_type, str(tmdb_id), imdb_id=imdb_id, season=season, episode=episode
        )
        payload = json.dumps(res.to_dict())
        async with AsyncSessionLocal() as db:
            row = await db.get(MediaSegment, key)
            if row:
                row.data = payload
            else:
                db.add(MediaSegment(cache_key=key, data=payload))
            await db.commit()
        log_service.info(f"segments cached for {key}: {payload}")
    except Exception as e:  # noqa: BLE001
        log_service.warning(f"segment detect failed {key}: {e}")
    finally:
        _inflight.discard(key)


@router.get("/{media_type}/{tmdb_id}")
async def get_segments(
    media_type: str,
    tmdb_id: int,
    season: Optional[int] = Query(None),
    episode: Optional[int] = Query(None),
    imdb_id: Optional[str] = Query(None),
    refresh: bool = Query(False),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Return cached intro/credits, or trigger background detection.

    Response: ``{"status": "ready", "duration", "intro", "credits", ...}`` when
    cached, else ``{"status": "pending"}`` (detection started).
    """
    key = _key(media_type, tmdb_id, season, episode)

    if not refresh:
        row = await db.get(MediaSegment, key)
        if row and row.data:
            return {"status": "ready", **json.loads(row.data)}

    if key not in _inflight:
        _inflight.add(key)
        asyncio.create_task(
            _detect_and_store(key, media_type, tmdb_id, imdb_id, season, episode)
        )
    return {"status": "pending"}
