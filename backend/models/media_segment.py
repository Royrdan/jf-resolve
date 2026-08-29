"""Cached intro/credits detection results (media segments).

One row per episode, keyed by ``{media_type}:{tmdb_id}:{season}:{episode}``.
``data`` is the JSON DetectResult from segment_service. Computed once (chromaprint
fingerprint is expensive-ish) and reused on every subsequent play, just like the
resolved-URL cache.
"""

from sqlalchemy import Column, DateTime, String, Text
from sqlalchemy.sql import func

from ..database import Base


class MediaSegment(Base):
    """Cached intro/credits segments for one episode."""

    __tablename__ = "media_segments"

    cache_key = Column(String(200), primary_key=True, index=True)
    data = Column(Text)  # JSON: {duration, intro, credits, method, reference}
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    def __repr__(self):
        return f"<MediaSegment {self.cache_key}>"
