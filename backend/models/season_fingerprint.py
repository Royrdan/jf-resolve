"""Per-season intro/credits fingerprint templates.

Debrid hands us a different release per episode/stream, so comparing one episode
against another is fragile (alignment + edit differences). Instead we learn a
stable *template* — the chromaprint fingerprint of the season's intro theme (and
credits theme when it recurs) — once, then match every future stream against that
template to locate the skip points in that specific file. Themes can change
between seasons, so it's keyed per season.
"""

from sqlalchemy import Column, DateTime, String, Text
from sqlalchemy.sql import func

from ..database import Base


class SeasonFingerprint(Base):
    """Learned intro/credits fingerprint templates for one season."""

    __tablename__ = "season_fingerprints"

    key = Column(String(200), primary_key=True, index=True)  # tv:tmdb:season
    intro_fp = Column(Text)    # JSON list[int] chromaprint hashes, or null
    credits_fp = Column(Text)  # JSON list[int], or null
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    def __repr__(self):
        return f"<SeasonFingerprint {self.key}>"
