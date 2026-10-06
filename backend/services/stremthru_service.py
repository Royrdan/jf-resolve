"""StremThru DMM-catalogue integration (Zilean's successor).

StremThru (MunifTanjim/stremthru) replaces our abandoned Zilean instance as
the self-hosted index of the DebridMediaManager hashlists. Unlike Zilean —
which we ran with IMDb import-matching disabled and therefore queried by
loose title text — StremThru is IMDb-id-centric: a background worker maps
every parsed torrent to an IMDb id against the official IMDb dataset, and
its Torznab endpoint looks torrents up by exact id (plus season/episode).
jf-resolve already resolves the IMDb id from TMDB, so we query by id and
sidestep the whole class of title-normalisation bugs Zilean had (see the
apostrophe saga in zilean_service.py).

This service is deliberately shaped like :class:`ZileanService`: the entry
points return the same **Stremio-style stream dicts** so the resolve pipeline
in ``stream.py`` (metadata filter → synthetic ``torrent://`` ref → cached-only
TorBox resolution) is completely source-agnostic.

Query contract (torznab, JSON output)::

    GET {base}/v0/torznab/api?t=movie&imdbid=<tt...>&o=json
    GET {base}/v0/torznab/api?t=tvsearch&imdbid=<tt...>&season=<s>&ep=<e>&o=json
      -> {"channel": {"items": [{"title": <release name>, "size": <bytes>,
            "attr": [{"@attributes": {"name": ..., "value": ...}}, ...]}]}}

Relevant attrs: infohash, resolution (2160p...), video (codec), language
(comma-joined), audio ("AAC | 5.1" — codec(s) then channel layout(s)),
seeders, year. StremThru has no hdr/dubbed/subbed/source-quality fields;
those stay at neutral defaults, which the candidate sorter treats as
"unknown" and never downgrades (it merges with release-name parsing by max).

A title query (``q=``) is only a fallback for when the IMDb id is missing —
StremThru internally resolves ``q`` to IMDb ids anyway (top-5 dataset match),
so the id path is strictly more precise.
"""

from typing import Dict, List, Optional
from urllib.parse import urlencode

import httpx

from .log_service import log_service


def _to_int(v) -> int:
    """Best-effort int parse; 0 when absent/unparseable so size/seeder
    signals stay neutral."""
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return 0


def _item_attrs(item: Dict) -> Dict[str, str]:
    """Flatten a torznab JSON item's attr list to {name: value}.

    Entries arrive as {"@attributes": {"name": ..., "value": ...}} (the XML
    attribute convention carried into JSON); tolerate a bare {name, value}
    shape too so a future serializer change doesn't zero us out silently.
    """
    out: Dict[str, str] = {}
    for a in item.get("attr") or []:
        if not isinstance(a, dict):
            continue
        inner = a.get("@attributes", a)
        name = inner.get("name")
        if name and name not in out:
            out[name] = str(inner.get("value") or "")
    return out


class StremThruService:
    """Search a self-hosted StremThru instance for torrent infohashes."""

    def __init__(self, base_url: str):
        base_url = (base_url or "").strip().rstrip("/")
        if base_url and not base_url.startswith(("http://", "https://")):
            base_url = f"http://{base_url}"
        self.base_url = base_url

    async def _search(
        self,
        t: str,
        imdb_id: Optional[str] = None,
        season: Optional[int] = None,
        episode: Optional[int] = None,
        query: Optional[str] = None,
    ) -> List[Dict]:
        """Call the torznab endpoint and normalise hits into Stremio-style
        dicts. Returns [] on any error so the caller can fall back cleanly."""
        if not self.base_url or not (imdb_id or query):
            return []

        params = {"t": t, "o": "json"}
        if imdb_id:
            params["imdbid"] = imdb_id
        elif query:
            params["q"] = query
        if season is not None:
            params["season"] = int(season)
        if episode is not None:
            params["ep"] = int(episode)
        url = f"{self.base_url}/v0/torznab/api?{urlencode(params)}"

        log_service.info(f"StremThru: querying {url}")

        try:
            # First query for a title triggers StremThru's on-demand mapping
            # job; the search itself answers from what is already mapped, so
            # keep the timeout modest like Zilean's.
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(url)
        except Exception as e:
            log_service.error(f"StremThru: request failed ({imdb_id or query}): {e}")
            return []

        if resp.status_code != 200:
            log_service.error(
                f"StremThru: returned {resp.status_code} for {imdb_id or query}"
                f" (S{season}E{episode})"
            )
            return []

        try:
            items = resp.json().get("channel", {}).get("items") or []
        except Exception as e:
            log_service.error(f"StremThru: bad JSON for {imdb_id or query}: {e}")
            return []

        streams: List[Dict] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            attrs = _item_attrs(item)
            info_hash = attrs.get("infohash")
            raw_title = item.get("title") or ""
            if not info_hash or not raw_title:
                continue
            resolution = attrs.get("resolution") or ""
            # "AAC | 5.1" → audio codecs vs channel layouts (either half may
            # be absent; a bare "AAC" has no separator).
            audio_raw = attrs.get("audio") or ""
            audio_part, _, channels_part = audio_raw.partition(" | ")
            languages = [
                s.strip() for s in (attrs.get("language") or "").split(",") if s.strip()
            ]
            # Stremio-shaped, identical contract to ZileanService: `title`
            # carries the release name (drives detect_quality, the metadata
            # filter, and the TorBox filename hint); no `url` — the infohash
            # ref is synthesised downstream and MUST go through cached-only
            # TorBox resolution.
            streams.append(
                {
                    "infoHash": str(info_hash).lower(),
                    "title": raw_title,
                    "name": f"StremThru {resolution}".strip(),
                    "fileIdx": None,
                    "behaviorHints": {"filename": raw_title},
                    "languages": languages,
                    # No source-quality (BluRay/WEB) field in torznab attrs;
                    # the sorter merges with release-name parsing by max, so
                    # an empty value never downgrades a name-detected signal.
                    "quality": "",
                    "codec": attrs.get("video") or "",
                    "audio": [s.strip() for s in audio_part.split(",") if s.strip()],
                    "channels": [
                        s.strip() for s in channels_part.split(",") if s.strip()
                    ],
                    "hdr": [],
                    "dubbed": False,
                    "subbed": False,
                    "sizeBytes": _to_int(attrs.get("size")) or _to_int(item.get("size")),
                }
            )

        log_service.info(
            f"StremThru: {len(streams)} usable candidate(s) for {imdb_id or query}"
            + (
                f" S{season:02d}E{episode:02d}"
                if season is not None and episode is not None
                else ""
            )
        )
        return streams

    async def get_movie_streams(
        self, imdb_id: Optional[str], title: Optional[str] = None
    ) -> List[Dict]:
        """Infohash candidates for a movie, by IMDb id (preferred) or title."""
        return await self._search("movie", imdb_id=imdb_id, query=title)

    async def get_episode_streams(
        self,
        imdb_id: Optional[str],
        season: int,
        episode: int,
        title: Optional[str] = None,
    ) -> List[Dict]:
        """Infohash candidates for a TV episode. Season/episode filter
        server-side; StremThru includes season packs when they cover the
        requested episode."""
        return await self._search(
            "tvsearch", imdb_id=imdb_id, season=season, episode=episode, query=title
        )
