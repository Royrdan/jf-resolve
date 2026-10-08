"""Service for auto-populating library and updating series"""

import asyncio
from itertools import zip_longest
from typing import Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models.library_item import LibraryItem
from .library_service import LibraryService
from .log_service import log_service
from .settings_manager import SettingsManager
from .tmdb_service import TMDBService

# TMDB watch-provider ids for the AU services we seed the library from.
# 8 Netflix | 21 Stan | 337 Disney+ | 119 Prime Video
# 350 Apple TV+ | 531 Paramount+ | 385 BINGE
DEFAULT_PROVIDERS = [8, 21, 337, 119, 350, 531, 385]

# The kids pass runs against child-focused catalogues only:
# 337 Disney+ | 175 Netflix Kids
DEFAULT_KIDS_PROVIDERS = [337, 175]

# TMDB genres Family (10751) or Kids (10762). Deliberately NOT Animation (16):
# on TMDB that genre also carries Family Guy, American Dad and Bob's Burgers.
DEFAULT_KIDS_GENRES = "10751|10762"


class PopulateService:
    """Handle automatic library population and series updates"""

    def __init__(
        self,
        db: AsyncSession,
        tmdb: TMDBService,
        library: LibraryService,
        settings: SettingsManager,
    ):
        self.db = db
        self.tmdb = tmdb
        self.library = library
        self.settings = settings

    async def run_auto_populate(self) -> Dict:
        """
        Fetch trending/popular content and add to library
        """
        # Read settings
        sources = await self.settings.get("populate_sources", ["popular"])
        limit = int(await self.settings.get("populate_limit", 5) or 0)
        excluded_ids_str = await self.settings.get("populate_excluded_ids", "")
        excluded_ids = [
            int(id_str.strip())
            for id_str in excluded_ids_str.split(",")
            if id_str.strip().isdigit()
        ]

        quality_versions = await self.settings.get(
            "populate_default_qualities", ["1080p"]
        )

        added_count = 0
        total_found = 0
        if isinstance(sources, str):
            sources = [sources]

        # The kids pass gets a reserved slice of tonight's budget, so run it
        # before the general pass can spend the lot on blockbusters.
        kids_limit = int(await self.settings.get("populate_kids_limit", 20) or 0)
        if "kids" in sources:
            sources = ["kids"] + [s for s in sources if s != "kids"]

        log_service.info(
            f"Starting auto-populate from sources: {sources} (Limit: {limit})"
        )

        for source in sources:
            if added_count >= limit:
                break

            # Ceiling this source may take the shared nightly budget up to.
            if source == "kids":
                source_limit = min(added_count + kids_limit, limit)
            else:
                source_limit = limit
            if added_count >= source_limit:
                continue

            items_with_type = []
            try:
                if source == "trending":
                    movie_trending = await self.tmdb.get_trending("movie")
                    tv_trending = await self.tmdb.get_trending("tv")
                    items_with_type = [
                        (item, item.get("media_type"))
                        for item in movie_trending.get("results", [])
                        + tv_trending.get("results", [])
                    ]
                elif source == "popular":
                    movie_pop = await self.tmdb.get_popular("movie")
                    tv_pop = await self.tmdb.get_popular("tv")
                    items_with_type = [
                        (item, "movie") for item in movie_pop.get("results", [])
                    ] + [(item, "tv") for item in tv_pop.get("results", [])]
                elif source == "top_rated":
                    movie_top = await self.tmdb.get_top_rated("movie")
                    tv_top = await self.tmdb.get_top_rated("tv")
                    items_with_type = [
                        (item, "movie") for item in movie_top.get("results", [])
                    ] + [(item, "tv") for item in tv_top.get("results", [])]
                elif source in ("providers", "kids"):
                    # Seed from the streaming services, one search per service
                    # per media type, most-popular first, English-only, and
                    # (per filter_unreleased) only actually-released titles.
                    items_with_type = await self._collect_provider_items(
                        kids=(source == "kids"),
                        budget=source_limit - added_count,
                        excluded_ids=excluded_ids,
                    )

                # Process items with their media types
                for item_data, media_type in items_with_type:
                    if added_count >= source_limit:
                        break

                    tmdb_id = item_data.get("id")

                    if not media_type or media_type not in ["movie", "tv"]:
                        continue

                    if tmdb_id in excluded_ids:
                        continue

                    if await self.library.is_in_library(tmdb_id, media_type):
                        continue
                    try:
                        await self.library.add_to_library(
                            tmdb_id=tmdb_id,
                            media_type=media_type,
                            quality_versions=quality_versions,
                            added_via="auto_populate",
                        )
                        added_count += 1
                        total_found += 1
                        log_service.info(f"Auto-populated {media_type} ID {tmdb_id}")
                    except Exception as e:
                        log_service.error(
                            f"Failed to auto-populate {media_type} {tmdb_id}: {e}"
                        )

            except Exception as e:
                log_service.error(f"Error fetching from source {source}: {e}")

        return {
            "success": True,
            "added_count": added_count,
            "message": f"Successfully added {added_count} items to library",
        }

    async def _collect_provider_items(
        self, kids: bool, budget: int, excluded_ids: List[int]
    ) -> List[tuple]:
        """
        Build tonight's candidate list from the streaming services.

        One search per service per media type — NOT one merged search across
        all of them. A merged, popularity-sorted search only ever surfaces the
        globally biggest titles, so a Disney+ family film never outranks the
        top of Netflix and Prime, and once those few hundred titles are in the
        library the search returns nothing new for good. Each service gets its
        own list here, and the lists are then interleaved so no single service
        can spend the whole nightly budget.
        """
        if kids:
            provider_ids = self._parse_ids(
                await self.settings.get(
                    "populate_kids_providers", DEFAULT_KIDS_PROVIDERS
                )
            )
            genres = await self.settings.get(
                "populate_kids_genres", DEFAULT_KIDS_GENRES
            )
            max_pages = int(await self.settings.get("populate_kids_pages", 10) or 10)
        else:
            provider_ids = self._parse_ids(
                await self.settings.get("populate_providers", DEFAULT_PROVIDERS)
            )
            genres = None
            max_pages = int(
                await self.settings.get("populate_provider_pages", 10) or 10
            )

        if not provider_ids or budget <= 0:
            return []

        region = await self.settings.get("populate_watch_region", "AU")
        released_only = await self.settings.get("filter_unreleased", True)
        english_only = await self.settings.get("populate_english_only", True)

        # One query for the whole library beats one per candidate — this walks
        # thousands of titles a night.
        result = await self.db.execute(
            select(LibraryItem.tmdb_id, LibraryItem.media_type)
        )
        in_library = {(row[0], row[1]) for row in result.all()}
        skip_ids = set(excluded_ids)

        media_types = ("movie", "tv")
        bucket_count = len(provider_ids) * len(media_types)
        # Each service's fair share of the budget, times three for headroom so
        # services that have run dry don't leave the budget unspent.
        needed = max(10, -(-budget // bucket_count) * 3)

        # Shared across every bucket: plenty of titles carry two or three
        # services (Brooklyn Nine-Nine is on both Netflix and Stan), and
        # without this the same title lands in several buckets and burns
        # interleave slots that another service could have used.
        claimed = set()

        buckets = []
        for provider_id in provider_ids:
            for media_type in media_types:
                fresh = await self._collect_candidates(
                    media_type=media_type,
                    provider_id=provider_id,
                    region=region,
                    released_only=released_only,
                    english_only=english_only,
                    genres=genres,
                    max_pages=max_pages,
                    needed=needed,
                    skip_ids=skip_ids,
                    in_library=in_library,
                    claimed=claimed,
                )
                log_service.info(
                    f"{'Kids' if kids else 'Provider'} {provider_id} {media_type}: "
                    f"{len(fresh)} candidates not yet in library"
                )
                buckets.append([(item, media_type) for item in fresh])

        return self._interleave(buckets)

    async def _collect_candidates(
        self,
        media_type: str,
        provider_id: int,
        region: str,
        released_only: bool,
        english_only: bool,
        genres: Optional[str],
        max_pages: int,
        needed: int,
        skip_ids: set,
        in_library: set,
        claimed: set,
    ) -> List[Dict]:
        """
        Page through one service's catalogue most-popular-first, keeping the
        titles that aren't in the library yet and stopping as soon as there are
        `needed` of them. A service whose top pages are already fully owned
        keeps paging, so the search window moves outward as the library fills
        instead of re-reading the same titles every night.
        """
        fresh: List[Dict] = []

        for page in range(1, max_pages + 1):
            try:
                res = await self.tmdb.discover_by_provider(
                    media_type,
                    [provider_id],
                    region,
                    released_only,
                    english_only,
                    page,
                    genres,
                )
            except Exception as e:
                log_service.error(
                    f"TMDB discover failed (provider {provider_id} {media_type} "
                    f"page {page}): {e}"
                )
                break

            results = res.get("results") or []
            if not results:
                break

            for item in results:
                # Stop exactly on target: anything added to `claimed` past
                # this point would be reserved and then thrown away, hiding
                # the title from every other bucket too.
                if len(fresh) >= needed:
                    break
                tmdb_id = item.get("id")
                if not tmdb_id or tmdb_id in skip_ids:
                    continue
                if (tmdb_id, media_type) in claimed:
                    continue
                if (tmdb_id, media_type) in in_library:
                    continue
                claimed.add((tmdb_id, media_type))
                fresh.append(item)

            if len(fresh) >= needed:
                break
            if page >= (res.get("total_pages") or 1):
                break

        return fresh

    @staticmethod
    def _interleave(buckets: List[List[tuple]]) -> List[tuple]:
        """
        Take one title from each service in turn, so the nightly budget is
        shared out rather than going to whichever service is most popular.
        """
        out: List[tuple] = []
        for row in zip_longest(*buckets):
            out.extend(item for item in row if item is not None)
        return out

    @staticmethod
    def _parse_ids(value) -> List[int]:
        """Accept a JSON list, a comma-separated string, or a single id."""
        if value is None:
            return []
        if isinstance(value, int):
            return [value]
        if isinstance(value, str):
            value = [part for part in value.replace(" ", "").split(",") if part]
        out: List[int] = []
        for item in value:
            try:
                out.append(int(item))
            except (TypeError, ValueError):
                continue
        return out

    async def run_series_update(self) -> Dict:
        """
        Check all series in library for new episodes
        """
        log_service.info("Starting manual series update check for all library items")

        result = await self.db.execute(
            select(LibraryItem).where(LibraryItem.media_type == "tv")
        )
        items = result.scalars().all()

        total_new_episodes = 0
        updated_series_count = 0

        for item in items:
            try:
                refresh_result = await self.library.refresh_item(item.id)
                new_count = refresh_result.get("new_episodes", 0)
                total_new_episodes += new_count
                if new_count > 0:
                    updated_series_count += 1
            except Exception as e:
                log_service.error(f"Failed to update series '{item.title}': {e}")

        return {
            "success": True,
            "updated_series_count": updated_series_count,
            "total_new_episodes": total_new_episodes,
            "message": f"Updated {updated_series_count} series, found {total_new_episodes} new episodes",
        }
